"""
Singleton TaskTracker instance and public API.

Provides the singleton TaskTracker instance and ensures only one
instance exists throughout the application.
"""

from .registry import FromModel
from .tracker import TaskTracker

task_tracker = TaskTracker()

__all__ = [
    "task_tracker",
    "FromModel",
]
