"""
Celery signal handlers for TaskTracker.

Manages all Celery signal connections and handles task lifecycle events.
Connects to various Celery signals to track task execution states.
"""

import logging
import traceback as tb_module
from typing import TYPE_CHECKING

from celery.exceptions import Reject
from celery.signals import (
    after_task_publish,
    task_failure,
    task_prerun,
    task_received,
    task_revoked,
    task_success,
)

if TYPE_CHECKING:  # avoid circular imports during runtime
    from celery_task_tracker.tracker import TaskTracker

logger = logging.getLogger(__name__)


def _safe_str(obj, fallback=""):
    """Safely convert object to string with fallback."""
    try:
        result = str(obj)
        return result if result else fallback
    except Exception:
        try:
            return repr(obj)
        except Exception:
            return fallback


def _safe_format_traceback(tb):
    """Safely format traceback object or string."""
    try:
        if isinstance(tb, str):
            return tb.strip() or None
        return "".join(tb_module.format_tb(tb)).strip() or None
    except Exception:
        return "<traceback formatting failed>"


def _format_exception_part(exception):
    """Format exception into error part string or None."""
    if exception is None:
        return None
    exc_msg = _safe_str(exception, fallback="<conversion failed>")
    return f"Exception: {exc_msg}" if exc_msg else None


def _format_traceback_part(traceback):
    """Format traceback into error part string or None."""
    if traceback is None:
        return None
    formatted_tb = _safe_format_traceback(traceback)
    return f"Traceback: {formatted_tb}" if formatted_tb else None


def _format_failure_info(exception, traceback):
    """Format exception and traceback into error info string."""
    parts = [p for p in (_format_exception_part(exception), _format_traceback_part(traceback)) if p]
    return " | ".join(parts) or "Unknown error"


class TaskTrackerSignalHandlers:
    """
    Manages Celery signal connections and task lifecycle event handling.

    Connects to various Celery signals and handles task lifecycle events
    by updating the task tracking storage.
    """

    def __init__(self, task_tracker):
        self.tracker: TaskTracker = task_tracker
        self._connect_signals()

    def _connect_signals(self):
        """Connect all Celery signals to their handlers"""
        after_task_publish.connect(self._handle_task_published)
        task_received.connect(self._handle_task_received)
        task_prerun.connect(self._handle_task_prerun)
        task_success.connect(self._handle_task_success)
        task_failure.connect(self._handle_task_failure)
        task_revoked.connect(self._handle_task_revoked)

    def _prepare_and_upsert(self, task_id, task_name, args, kwargs, state, result=None):
        models = self.tracker.registry.get_models_for_task(task_name)
        objects = []
        for model_label in models:
            cfg = self.tracker.registry.get_config_for_task(task_name, model_label)
            obj_id = None
            try:
                if cfg and cfg.can_resolve_id_from_args(args, kwargs):
                    obj_id = cfg.resolve_object_id_from_args(args, kwargs, model_label)
                else:
                    obj_id = self.tracker.storage.resolve_object_id(task_id, model_label)
                    if obj_id is None and cfg:
                        obj_id = cfg.resolve_object_id_from_args(args, kwargs, model_label)
            except Exception:
                obj_id = None
            if obj_id is not None:
                objects.append((model_label, obj_id))
        self.tracker.storage.upsert_task_record(task_id, task_name, state, objects, result)

    def _handle_task_published(self, sender=None, headers=None, body=None, **kwds):
        """Handle task publication - create task record with Pending state"""
        task_id = headers.get("id")
        task_name = headers.get("task")
        task_args = body[0] if body and len(body) > 0 else ()
        task_kwargs = body[1] if body and len(body) > 1 else {}

        self._prepare_and_upsert(task_id, task_name, task_args, task_kwargs, "Pending")

    def _handle_task_received(self, sender=None, request=None, **kwds):
        """Handle task received - update state to Received"""
        task_id = getattr(request, "id", None)
        task_name = getattr(request, "task_name", None)
        task_args = getattr(request, "args", [])
        task_kwargs = getattr(request, "kwargs", {})

        self._prepare_and_upsert(task_id, task_name, task_args, task_kwargs, "Received")

    def _handle_task_prerun(self, sender=None, task_id=None, task=None, args=None, kwargs=None, **kwds):
        """Handle task start - update state to Started"""
        task_name = getattr(task, "name", None)
        task_args = args if args else []
        task_kwargs = kwargs if kwargs else {}

        self._prepare_and_upsert(task_id, task_name, task_args, task_kwargs, "Started")
        self.tracker.storage.update_revoke_request(task_id, False)

    def _handle_task_success(self, sender=None, result=None, **kwds):
        """Handle task success - update state to Success"""
        task = sender
        task_id = getattr(task, "request", {}).get("id", None)
        task_name = getattr(task, "name", None)
        task_args = getattr(task, "args", [])
        task_kwargs = getattr(task, "kwargs", {})

        self._prepare_and_upsert(task_id, task_name, task_args, task_kwargs, "Success", str(result))

    def _handle_task_failure(
        self,
        sender=None,
        task_id=None,
        exception=None,
        args=None,
        kwargs=None,
        traceback=None,
        einfo=None,
        **kwds,
    ):
        """Handle task failure - update state to Failure"""
        task = sender
        task_name = getattr(task, "name", None)
        task_args = args if args else []
        task_kwargs = kwargs if kwargs else {}

        state = "Rejected" if isinstance(exception, Reject) else "Failure"
        error_info = _format_failure_info(exception, traceback)

        self._prepare_and_upsert(task_id, task_name, task_args, task_kwargs, state, error_info)

    def _handle_task_revoked(self, sender=None, request=None, **kwargs):
        """Handle task revocation - update state to Revoked"""
        task_id = getattr(request, "id", None)
        task_name = getattr(request, "task", None)
        task_args = getattr(request, "args", [])
        task_kwargs = getattr(request, "kwargs", {})

        self._prepare_and_upsert(task_id, task_name, task_args, task_kwargs, "Revoked")
        self.tracker.storage.update_revoke_request(task_id, False)
