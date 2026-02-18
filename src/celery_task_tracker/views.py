import logging

from celery import current_app
from django.apps import apps
from django.contrib import admin
from django.contrib.admin.views.decorators import staff_member_required
from django.http import Http404, HttpResponseRedirect, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_protect

from .config import TaskTrackerConfig
from .task_tracker import TaskTracker, task_tracker

task_tracker: TaskTracker


logger = logging.getLogger(__name__)


class BaseTaskTrackerView(View):
    """Base view with common functionality for TaskTracker views"""

    def _create_error_response(self, message, status=400, simple=False):
        """Create standardized error response"""
        response_data = {"error": message} if simple else {"status": "error", "message": message}
        return JsonResponse(response_data, status=status)

    def _parse_admin_url(self, request):
        """Parse admin URL to extract app_label, model_name, and object_id"""
        url_parts = request.path.strip("/").split("/")
        if len(url_parts) >= 4 and url_parts[0] == "admin":
            return url_parts[1], url_parts[2], url_parts[3]
        else:
            raise Http404("Invalid URL structure")


@method_decorator([staff_member_required, csrf_protect], name="dispatch")
class TaskTrackerView(BaseTaskTrackerView):
    def get(self, request, **kwargs):
        app_label, model_name, object_id = self._parse_admin_url(request)
        model_class = apps.get_model(app_label, model_name)
        obj = get_object_or_404(model_class, pk=object_id)

        model_label = model_class._meta.label

        available_tasks = task_tracker.registry.get_tasks_for_model(model_label)
        base_url = f"/admin/{app_label}/{model_name}/{object_id}/task-tracker/"

        context = {
            "object": obj,
            "original": obj,
            "opts": model_class._meta,
            "available_apps": admin.site.get_app_list(request),
            "available_tasks": available_tasks,
            "task_launch_url": f"{base_url}launch/",
            "task_list_url": f"{base_url}list/",
            "task_revoke_url": f"{base_url}revoke/",
            "task_data_url": f"{base_url}tasks/",
        }

        return render(request, "admin/celery_task_tracker/task_tracker.html", context)


@method_decorator([staff_member_required], name="dispatch")
class TaskListView(BaseTaskTrackerView):
    def get(self, request, **kwargs):
        app_label, model_name, object_id = self._parse_admin_url(request)
        model_class = apps.get_model(app_label, model_name)
        obj = get_object_or_404(model_class, pk=object_id)
        model_label = model_class._meta.label

        filter_type = request.GET.get("filter", "none")
        filter_value = request.GET.get("value", None)
        page = int(request.GET.get("page", 1))
        page_size = min(
            int(request.GET.get("page_size", TaskTrackerConfig.PAGE_SIZE)),
            TaskTrackerConfig.MAX_PAGE_SIZE,
        )

        task_tracker.cleanup_expired_tasks(model_label, obj.pk)

        result = task_tracker.storage.list_index(
            model_label,
            obj.pk,
            filter_type if filter_type in ["task", "state"] else "tasks",
            filter_value,
            page=page,
            page_size=page_size,
        )
        tasks = result["items"]
        total_count = result["total"]

        return JsonResponse(
            {
                "tasks": tasks,
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "has_next": page * page_size < total_count,
                "has_previous": page > 1,
            }
        )


@method_decorator([staff_member_required, csrf_protect], name="dispatch")
class TaskLaunchView(BaseTaskTrackerView):
    def post(self, request, **_):
        app_label, model_name, object_id = self._parse_admin_url(request)
        model_class = apps.get_model(app_label, model_name)
        obj = get_object_or_404(model_class, pk=object_id)

        task_name = request.GET.get("task")
        if not task_name:
            return self._create_error_response("Task name is required.", 400, True)

        model_label = model_class._meta.label
        cfg = task_tracker.registry.get_config_for_task(task_name, model_label)
        if not cfg:
            return self._create_error_response("Task not found.", 404, True)

        task_args, task_kwargs = cfg.build_task_args_from_instance(obj)
        result = cfg.launch_task(*task_args, **task_kwargs)
        task_id = getattr(result, "id", None) if result else None

        if not task_id:
            return self._create_error_response("Couldn't launch the task.", 500, True)

        message = f'Launched "{task_name}" [{task_id}] successfully'
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return JsonResponse({"status": "ok", "task_id": task_id, "message": message}, status=200)

        return HttpResponseRedirect(f"/admin/{app_label}/{model_name}/{object_id}/task-tracker/")


@method_decorator([staff_member_required, csrf_protect], name="dispatch")
class TaskRevokeView(BaseTaskTrackerView):
    def post(self, request, **kwargs):
        app_label, model_name, object_id = self._parse_admin_url(request)
        task_id = request.POST.get("task_id")
        if not task_id:
            return self._create_error_response("Task ID is required.", simple=True)

        task_data = task_tracker.storage.get_task(task_id)
        if not task_data:
            return self._create_error_response("Task not found", 404, simple=True)

        state = task_data.get("state")
        if state not in {"Pending", "Received", "Started"}:
            return self._create_error_response(f"Cannot revoke task in state: {state}", simple=True)

        if task_data.get("revoke_requested"):
            return self._create_error_response("Revoke request already pending for this task", simple=True)

        task_tracker.storage.update_revoke_request(task_id, True)
        current_app.control.revoke(task_id, terminate=(state == "Started"))
        return HttpResponseRedirect(f"/admin/{app_label}/{model_name}/{object_id}/task-tracker/")


@method_decorator([staff_member_required], name="dispatch")
class TaskDataView(BaseTaskTrackerView):
    """Unified endpoint for getting task result, error, or reason details"""

    def get(self, _, task_id, **kwargs):
        task_data = task_tracker.storage.get_task(task_id)
        if not task_data:
            return self._create_error_response("Task not found", 404, simple=True)

        return JsonResponse(task_data)
