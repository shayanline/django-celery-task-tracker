from django.conf import settings


def load_setting(setting_name, default_value):
    """
    Load a setting from Django settings with fallback to default value.

    Returns default_value if:
    - The setting is not defined in settings
    - The setting is defined but has None value
    """
    value = getattr(settings, setting_name, default_value)
    return default_value if value is None else value


class TaskTrackerConfig:
    RETENTION_WINDOW = load_setting("TASK_TRACKER_RETENTION_WINDOW", 86400)
    PAGE_SIZE = load_setting("TASK_TRACKER_PAGE_SIZE", 10)
    MAX_PAGE_SIZE = load_setting("TASK_TRACKER_MAX_PAGE_SIZE", 200)
    REDIS_URL = load_setting("CELERY_TASKTRACKER_REDIS_URL", "redis://")

    STATE_PRIORITIES = {
        "Success": 5,
        "Failure": 5,
        "Revoked": 4,
        "Rejected": 4,
        "Started": 3,
        "Received": 2,
        "Pending": 1,
    }

    TASK_STATES = list(STATE_PRIORITIES.keys())
