import sys
from unittest import TestCase
from unittest.mock import Mock

from celery.exceptions import Reject

from celery_task_tracker.signals import TaskTrackerSignalHandlers
from celery_task_tracker.tracker import TaskTracker


class TestTaskTrackerSignalHandlers(TestCase):
    """Test cases for TaskTracker signal handlers"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_tracker = Mock(spec=TaskTracker)
        self.mock_tracker.registry = Mock()
        self.mock_tracker.storage = Mock()
        self.mock_tracker.registry.get_models_for_task.return_value = []

        self.signal_handlers = TaskTrackerSignalHandlers(self.mock_tracker)

    def test_handle_task_failure_with_string_traceback(self):
        """Test that string traceback (TimeLimitExceeded case) is handled correctly"""
        mock_task = Mock()
        mock_task.name = "test_task"

        exception = Exception("TimeLimitExceeded")
        string_traceback = "Traceback string representation"

        self.signal_handlers._handle_task_failure(
            sender=mock_task,
            task_id="test-task-id",
            exception=exception,
            args=[],
            kwargs={},
            traceback=string_traceback,
            einfo=None,
        )

        self.mock_tracker.storage.upsert_task_record.assert_called_once()
        call_args = self.mock_tracker.storage.upsert_task_record.call_args
        self.assertEqual(call_args[0][2], "Failure")
        error_info = call_args[0][4]
        self.assertIn("Exception: TimeLimitExceeded", error_info)
        self.assertIn("Traceback: Traceback string representation", error_info)

    def test_handle_task_failure_with_traceback_object(self):
        """Test that traceback object is formatted correctly"""
        mock_task = Mock()
        mock_task.name = "test_task"

        exception = Exception("Test exception")

        try:
            raise ValueError("Test error")
        except ValueError:
            exc_type, exc_value, traceback_obj = sys.exc_info()

        self.signal_handlers._handle_task_failure(
            sender=mock_task,
            task_id="test-task-id",
            exception=exception,
            args=[],
            kwargs={},
            traceback=traceback_obj,
            einfo=None,
        )

        self.mock_tracker.storage.upsert_task_record.assert_called_once()
        call_args = self.mock_tracker.storage.upsert_task_record.call_args
        self.assertEqual(call_args[0][2], "Failure")
        error_info = call_args[0][4]
        self.assertIsInstance(error_info, str)
        self.assertIn("Exception: Test exception", error_info)
        self.assertIn("Traceback:", error_info)
        self.assertIn("File", error_info)
        self.assertIn("test_handle_task_failure_with_traceback_object", error_info)

    def test_handle_task_failure_with_none_traceback(self):
        """Test that None traceback falls back to exception string"""
        mock_task = Mock()
        mock_task.name = "test_task"

        exception = Exception("Test exception message")

        self.signal_handlers._handle_task_failure(
            sender=mock_task,
            task_id="test-task-id",
            exception=exception,
            args=[],
            kwargs={},
            traceback=None,
            einfo=None,
        )

        self.mock_tracker.storage.upsert_task_record.assert_called_once()
        call_args = self.mock_tracker.storage.upsert_task_record.call_args
        self.assertEqual(call_args[0][2], "Failure")
        error_info = call_args[0][4]
        self.assertEqual(error_info, "Exception: Test exception message")

    def test_handle_task_failure_with_reject_exception(self):
        """Test that Reject exception results in Rejected state"""
        mock_task = Mock()
        mock_task.name = "test_task"

        exception = Reject("Task rejected")
        string_traceback = "Reject traceback"

        self.signal_handlers._handle_task_failure(
            sender=mock_task,
            task_id="test-task-id",
            exception=exception,
            args=[],
            kwargs={},
            traceback=string_traceback,
            einfo=None,
        )

        self.mock_tracker.storage.upsert_task_record.assert_called_once()
        call_args = self.mock_tracker.storage.upsert_task_record.call_args
        self.assertEqual(call_args[0][2], "Rejected")
        error_info = call_args[0][4]
        self.assertIn("Exception:", error_info)
        self.assertIn("Traceback: Reject traceback", error_info)

    def test_handle_task_failure_with_none_exception_and_traceback(self):
        """Test handling when both exception and traceback are None"""
        mock_task = Mock()
        mock_task.name = "test_task"

        self.signal_handlers._handle_task_failure(
            sender=mock_task,
            task_id="test-task-id",
            exception=None,
            args=[],
            kwargs={},
            traceback=None,
            einfo=None,
        )

        self.mock_tracker.storage.upsert_task_record.assert_called_once()
        call_args = self.mock_tracker.storage.upsert_task_record.call_args
        self.assertEqual(call_args[0][2], "Failure")
        self.assertEqual(call_args[0][4], "Unknown error")

    def test_handle_task_failure_with_empty_string_traceback(self):
        """Test that empty string traceback is filtered out"""
        mock_task = Mock()
        mock_task.name = "test_task"

        exception = Exception("Test exception")

        self.signal_handlers._handle_task_failure(
            sender=mock_task,
            task_id="test-task-id",
            exception=exception,
            args=[],
            kwargs={},
            traceback="",
            einfo=None,
        )

        self.mock_tracker.storage.upsert_task_record.assert_called_once()
        call_args = self.mock_tracker.storage.upsert_task_record.call_args
        self.assertEqual(call_args[0][2], "Failure")
        error_info = call_args[0][4]
        self.assertEqual(error_info, "Exception: Test exception")
        self.assertNotIn("Traceback:", error_info)

    def test_handle_task_failure_with_whitespace_only_traceback(self):
        """Test that whitespace-only traceback is filtered out"""
        mock_task = Mock()
        mock_task.name = "test_task"

        exception = Exception("Test exception")

        self.signal_handlers._handle_task_failure(
            sender=mock_task,
            task_id="test-task-id",
            exception=exception,
            args=[],
            kwargs={},
            traceback="   \n\t  ",
            einfo=None,
        )

        self.mock_tracker.storage.upsert_task_record.assert_called_once()
        call_args = self.mock_tracker.storage.upsert_task_record.call_args
        self.assertEqual(call_args[0][2], "Failure")
        error_info = call_args[0][4]
        self.assertEqual(error_info, "Exception: Test exception")
        self.assertNotIn("Traceback:", error_info)

    def test_handle_task_failure_with_non_convertible_exception(self):
        """Test handling of exception that cannot be converted to string"""

        class BadExceptionError(Exception):
            def __str__(self):
                raise RuntimeError("Cannot convert to string")

            def __repr__(self):
                return "BadExceptionError()"

        mock_task = Mock()
        mock_task.name = "test_task"

        exception = BadExceptionError()

        self.signal_handlers._handle_task_failure(
            sender=mock_task,
            task_id="test-task-id",
            exception=exception,
            args=[],
            kwargs={},
            traceback=None,
            einfo=None,
        )

        self.mock_tracker.storage.upsert_task_record.assert_called_once()
        call_args = self.mock_tracker.storage.upsert_task_record.call_args
        self.assertEqual(call_args[0][2], "Failure")
        error_info = call_args[0][4]
        self.assertIn("Exception: BadExceptionError()", error_info)

    def test_handle_task_failure_with_completely_non_convertible_exception(self):
        """Test handling of exception where both str and repr fail"""

        class VeryBadExceptionError(Exception):
            def __str__(self):
                raise RuntimeError("Cannot convert to string")

            def __repr__(self):
                raise RuntimeError("Cannot convert to repr")

        mock_task = Mock()
        mock_task.name = "test_task"

        exception = VeryBadExceptionError()

        self.signal_handlers._handle_task_failure(
            sender=mock_task,
            task_id="test-task-id",
            exception=exception,
            args=[],
            kwargs={},
            traceback=None,
            einfo=None,
        )

        self.mock_tracker.storage.upsert_task_record.assert_called_once()
        call_args = self.mock_tracker.storage.upsert_task_record.call_args
        self.assertEqual(call_args[0][2], "Failure")
        error_info = call_args[0][4]
        self.assertIn("Exception: <conversion failed>", error_info)

    def test_handle_task_failure_with_invalid_traceback_object(self):
        """Test handling of invalid traceback object"""
        mock_task = Mock()
        mock_task.name = "test_task"

        exception = Exception("Test exception")
        invalid_traceback = object()

        self.signal_handlers._handle_task_failure(
            sender=mock_task,
            task_id="test-task-id",
            exception=exception,
            args=[],
            kwargs={},
            traceback=invalid_traceback,
            einfo=None,
        )

        self.mock_tracker.storage.upsert_task_record.assert_called_once()
        call_args = self.mock_tracker.storage.upsert_task_record.call_args
        self.assertEqual(call_args[0][2], "Failure")
        error_info = call_args[0][4]
        self.assertIn("Exception: Test exception", error_info)
        self.assertIn("Traceback: <traceback formatting failed>", error_info)

    def test_handle_task_failure_with_only_traceback_no_exception(self):
        """Test handling when only traceback is provided without exception"""
        mock_task = Mock()
        mock_task.name = "test_task"

        string_traceback = "Traceback from somewhere"

        self.signal_handlers._handle_task_failure(
            sender=mock_task,
            task_id="test-task-id",
            exception=None,
            args=[],
            kwargs={},
            traceback=string_traceback,
            einfo=None,
        )

        self.mock_tracker.storage.upsert_task_record.assert_called_once()
        call_args = self.mock_tracker.storage.upsert_task_record.call_args
        self.assertEqual(call_args[0][2], "Failure")
        error_info = call_args[0][4]
        self.assertEqual(error_info, "Traceback: Traceback from somewhere")
        self.assertNotIn("Exception:", error_info)
