from django.urls import path

from .views import (
    TaskDataView,
    TaskLaunchView,
    TaskListView,
    TaskRevokeView,
    TaskTrackerView,
)

app_name = "celery_task_tracker"

urlpatterns = [
    path(
        "",
        TaskTrackerView.as_view(),
        name="task_tracker",
    ),
    path(
        "launch/",
        TaskLaunchView.as_view(),
        name="task_launch",
    ),
    path(
        "list/",
        TaskListView.as_view(),
        name="task_list",
    ),
    path(
        "revoke/",
        TaskRevokeView.as_view(),
        name="task_revoke",
    ),
    path(
        "tasks/<str:task_id>/",
        TaskDataView.as_view(),
        name="task_data",
    ),
]
