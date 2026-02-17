"""
Main TaskTracker orchestrator class.

Coordinates all components of the task tracking system and provides
the decorator interface for registering tasks with their associated models.
"""

import logging
from typing import Dict, Optional, Type, Union

from celery_task_tracker.utils import singleton

from .config import TaskTrackerConfig
from .registry import TaskConfig, TaskRegistry
from .signals import TaskTrackerSignalHandlers
from .storage import TaskTrackerRedisStorage

logger = logging.getLogger(__name__)


@singleton
class TaskTracker:
    """Orchestrates task tracking functionality."""

    def __init__(self):
        self.config = TaskTrackerConfig
        self.registry = TaskRegistry()
        self.storage = TaskTrackerRedisStorage()
        self.signal_handlers = TaskTrackerSignalHandlers(self)

    def register(
        self,
        model_class: Type,
        id_query: Optional[Union[Dict[str, str], str]] = None,
        launch_args: Optional[tuple] = None,
        launch_kwargs: Optional[dict] = None,
        hidden: bool = False,
    ):
        """Decorator to register tasks for a model.

        Args:
            model_class: The Django model class to associate the task with.
            id_query: How to resolve the model instance ID from task arguments.
            launch_args: Tuple of FieldReference or literals for positional arguments.
            launch_kwargs: Dictionary of keyword arguments for the task.
            hidden: If True, the task will not appear in the task tracker UI's
                "Available tasks" section but will still be tracked.
        """

        def decorator(task_func):
            task_name = task_func.name

            # Avoid duplicate registrations
            if self.registry.is_task_registered(task_name, model_class):
                return task_func

            self.registry.register_task(
                model_class,
                task_name,
                TaskConfig(task_func, id_query, launch_args, launch_kwargs, hidden),
            )
            return task_func

        return decorator

    def cleanup_expired_tasks(self, model_label: str, object_id):
        tasks = self.registry.get_tasks_for_model(model_label)
        self.storage.cleanup_expired_tasks(model_label, object_id, tasks)
