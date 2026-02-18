from importlib import import_module

from django.apps import AppConfig, apps


class CeleryTaskTrackerConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "celery_task_tracker"

    def ready(self):
        # Import task_tracker to initialize signal handlers
        from . import task_tracker  # noqa: F401

        # Auto-discover tasks in installed apps (needs to be in tasks.py files)
        self._autodiscover_tasks()

    def _autodiscover_tasks(self):
        """Auto-discover and import tasks modules from all installed apps."""
        for app_config in apps.get_app_configs():
            try:
                import_module(f"{app_config.name}.tasks")
            except ImportError:
                pass
