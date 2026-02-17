from unittest import TestCase
from unittest.mock import patch

from django.conf import LazySettings

from celery_task_tracker.config import load_setting


class TestConfig(TestCase):
    def test_load_setting_returns_default_when_settings_not_configured(self):
        lazy_settings = LazySettings()
        self.assertFalse(lazy_settings.configured)

        with patch("celery_task_tracker.config.settings", lazy_settings):
            self.assertEqual(load_setting("TASK_TRACKER_RETENTION_WINDOW", 123), 123)

    def test_load_setting_returns_default_when_setting_is_none(self):
        lazy_settings = LazySettings()
        self.assertFalse(lazy_settings.configured)
        lazy_settings.configure(TASK_TRACKER_RETENTION_WINDOW=None)

        with patch("celery_task_tracker.config.settings", lazy_settings):
            self.assertEqual(load_setting("TASK_TRACKER_RETENTION_WINDOW", 123), 123)
