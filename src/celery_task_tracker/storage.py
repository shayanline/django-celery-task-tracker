import datetime
import json
import logging
import time

import redis

from .config import TaskTrackerConfig

logger = logging.getLogger(__name__)


class TaskTrackerRedisStorage:
    """Handles all Redis operations for task tracking storage."""

    def __init__(self):
        self.redis_client = redis.from_url(TaskTrackerConfig.REDIS_URL, decode_responses=True)
        self.retention_window = int(TaskTrackerConfig.RETENTION_WINDOW)

    def get_index_key(self, model_label, object_id, index_type, value=None):
        if index_type == "tasks":
            return self._build_key(model_label, object_id, "tasks")
        if index_type in {"task", "state"} and value is not None:
            return self._build_key(model_label, object_id, index_type, value)
        raise ValueError("Invalid index_type or missing value.")

    def get_task(self, task_id):
        data = self.redis_client.hgetall(task_id)
        if not data:
            return None
        return self._parse_task_record(data)

    def list_index(self, model_label, object_id, index_type, value, page=1, page_size=25):
        key = self.get_index_key(model_label, object_id, index_type, value)
        start = (page - 1) * page_size
        end = start + page_size - 1
        pipe = self.redis_client.pipeline()
        pipe.zrevrange(key, start, end)
        pipe.zcard(key)
        ids, total = pipe.execute()
        if not ids:
            return {"items": [], "page": page, "page_size": page_size, "total": total}
        # Batch fetch all hashes in one pipeline
        pipe = self.redis_client.pipeline()
        for tid in ids:
            pipe.hgetall(tid)
        results = pipe.execute()
        items = []
        for data in results:
            if data:
                parsed = self._parse_task_record(data)
                items.append(parsed)
        return {"items": items, "page": page, "page_size": page_size, "total": total}

    def upsert_task_record(self, task_id, name, state, objects, result=None):
        """Atomically update the task hash (watching the task key);
        index updates are applied separately afterwards.
        """
        now_ms = int(time.time() * 1000)
        max_retries = 3
        for _ in range(max_retries):
            pipe = self.redis_client.pipeline()
            try:
                # Watch only the task hash key to make that update atomic.
                pipe.watch(task_id)
                existing = pipe.hgetall(task_id)

                if existing:
                    # Respect configured state priorities to avoid downgrading
                    current_priority = TaskTrackerConfig.STATE_PRIORITIES.get(existing.get("state", ""), 0)
                    new_priority = TaskTrackerConfig.STATE_PRIORITIES.get(state, 0)
                    if current_priority > new_priority:
                        pipe.unwatch()
                        return False

                hash_map = {
                    "id": task_id,
                    "name": name,
                    "state": state,
                    "objects": json.dumps(objects),
                    "created_at": str(existing.get("created_at", now_ms) if existing else now_ms),
                    "updated_at": str(now_ms),
                    "revoke_requested": (
                        existing.get("revoke_requested", json.dumps(False)) if existing else json.dumps(False)
                    ),
                }
                if result is not None:
                    hash_map["result"] = self._serialize_result(result)

                pipe.multi()
                pipe.hset(task_id, mapping=hash_map)
                pipe.expire(task_id, self.retention_window)
                pipe.execute()
                break
            except redis.WatchError as e:
                logger.warning(f"WatchError on upsert_task_record: {e}")
                continue
            finally:
                try:
                    pipe.reset()
                except Exception as e:
                    logger.error(f"Error resetting Redis pipeline: {e}")

        else:
            return False

        pipe = self.redis_client.pipeline()
        idx_ops = self._prepare_index_ops(name, state, objects)
        created_at_ms = int(hash_map.get("created_at", now_ms))
        self._queue_index_updates(pipe, idx_ops, task_id, created_at_ms)
        pipe.execute()
        return True

    def update_revoke_request(self, task_id, revoke_requested):
        max_retries = 3
        for _ in range(max_retries):
            pipe = self.redis_client.pipeline()
            try:
                pipe.watch(task_id)
                existing = pipe.hgetall(task_id)
                if not existing:
                    pipe.unwatch()
                    return

                pipe.multi()
                pipe.hset(
                    task_id,
                    mapping={
                        "revoke_requested": json.dumps(revoke_requested),
                        "updated_at": str(int(time.time() * 1000)),
                    },
                )
                pipe.expire(task_id, self.retention_window)
                pipe.execute()
                break
            except redis.WatchError as e:
                logger.warning(f"WatchError on update_revoke_request: {e}")
                continue
            finally:
                try:
                    pipe.reset()
                except Exception as e:
                    logger.error(f"Error resetting Redis pipeline: {e}")

    def cleanup_expired_tasks(self, model_label, object_id, registered_tasks):
        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - (self.retention_window * 1000)

        indexes = (
            [self.get_index_key(model_label, object_id, "tasks")]
            + [self.get_index_key(model_label, object_id, "state", s) for s in TaskTrackerConfig.TASK_STATES]
            + [self.get_index_key(model_label, object_id, "task", t) for t in registered_tasks]
        )

        pipe = self.redis_client.pipeline()
        for key in indexes:
            pipe.zremrangebyscore(key, "-inf", cutoff_ms)
            pipe.expire(key, self.retention_window)
        pipe.execute()

    def resolve_object_id(self, task_id, model_label):
        """Get the object's ID for a given model_label from the task hash."""
        objects = self.redis_client.hget(task_id, "objects")
        if not objects:
            return None
        for label, obj_id in json.loads(objects):
            if label == model_label:
                return obj_id
        return None

    def _build_key(self, model_label, object_id, *parts):
        base = f"{model_label.lower()}:{object_id}"
        if parts:
            return f"{base}:{':'.join(str(p) for p in parts)}"
        return base

    def _parse_task_record(self, data):
        parsed = dict(data)
        parsed["objects"] = json.loads(parsed.get("objects", "[]"))
        parsed["revoke_requested"] = json.loads(parsed.get("revoke_requested", "false"))
        parsed["result"] = json.loads(parsed.get("result", "null"))
        created_at = int(parsed.get("created_at", "0"))
        updated_at = int(parsed.get("updated_at", "0"))
        parsed["created_at"] = (
            datetime.datetime.fromtimestamp(created_at / 1000).strftime("%Y-%m-%d %H:%M:%S") if created_at else None
        )
        parsed["updated_at"] = (
            datetime.datetime.fromtimestamp(updated_at / 1000).strftime("%Y-%m-%d %H:%M:%S") if updated_at else None
        )
        return parsed

    def _prepare_index_ops(self, name, state, objects):
        idx_ops = []
        for model_label, object_id in objects:
            if object_id is None:
                continue
            tasks_key = self.get_index_key(model_label, object_id, "tasks")
            state_key = self.get_index_key(model_label, object_id, "state", state)
            task_key = self.get_index_key(model_label, object_id, "task", name)
            other_state_keys = [
                self.get_index_key(model_label, object_id, "state", s)
                for s in TaskTrackerConfig.TASK_STATES
                if s != state
            ]
            idx_ops.append((tasks_key, state_key, task_key, other_state_keys))
        return idx_ops

    def _queue_index_updates(self, pipe, idx_ops, task_id, created_at_ms):
        ts = int(created_at_ms)
        for tasks_key, state_key, task_key, other_state_keys in idx_ops:
            pipe.zadd(tasks_key, {task_id: ts})
            pipe.zadd(state_key, {task_id: ts})
            pipe.zadd(task_key, {task_id: ts})
            pipe.expire(tasks_key, self.retention_window)
            pipe.expire(state_key, self.retention_window)
            pipe.expire(task_key, self.retention_window)
            for k in other_state_keys:
                pipe.zrem(k, task_id)

    def _serialize_result(self, result):
        """Serialize task result to JSON"""
        try:
            return json.dumps(result)
        except (TypeError, ValueError):
            return json.dumps(str(result))
