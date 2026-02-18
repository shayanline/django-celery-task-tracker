"""
Task registration and model associations.

Manages task registration and their associations with Django models.
Ensures tasks are registered only once and provides a centralized registry.
Patch Django admin classes to include task tracking functionality.
"""

import inspect
import logging
import types
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple, Type

from celery import Task
from django.apps import apps
from django.contrib import admin
from django.contrib.admin import ModelAdmin
from django.db.models import Model
from django.template import TemplateDoesNotExist
from django.template.loader import get_template
from django.utils.html import format_html

logger = logging.getLogger(__name__)


@dataclass
class FieldReference:
    """Reference to a model field, supporting nested relations (e.g., 'company.name')."""

    field_path: str

    def resolve(self, instance) -> Any:
        value = instance
        for attr in self.field_path.split("."):
            value = getattr(value, attr, None)
            if value is None:
                return None
        return value() if callable(value) else value


FromModel = FieldReference


class TaskConfig:
    """
    Stores configuration for mapping model instances to Celery task calls.

    Attributes:
        id_query: Specifies how to resolve the model instance ID from task arguments (None, str, iterable, or dict).
        launch_args: Tuple of FieldReference or literals for positional arguments to the task.
        launch_kwargs: Dictionary of keyword arguments (FieldReference or literals) for the task.
        hidden: If True, the task will not appear in the task tracker UI's "Available tasks" section.
        _pos_names: List of positional parameter names extracted from the task function signature.
    """

    def __init__(
        self,
        task_func: Task,
        id_query: Optional[Any] = None,
        launch_args: Optional[Tuple[Any, ...]] = None,
        launch_kwargs: Optional[Dict[str, Any]] = None,
        hidden: bool = False,
    ):
        sig = inspect.signature(task_func)
        self.task_func = task_func
        self.id_query = id_query
        self.launch_args = tuple(launch_args) if launch_args else ()
        self.launch_kwargs = dict(launch_kwargs or {})
        self.hidden = hidden

        self._pos_names = [
            p.name for p in sig.parameters.values() if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]

    def launch_task(self, *args, **kwargs):
        """Public API: launch the Celery task with given args and kwargs."""
        return self.task_func.apply_async(args=args, kwargs=kwargs)

    def build_task_args_from_instance(self, model_instance):
        """Public API: build (args, kwargs) to call the Celery task for instance.

        Returns a tuple (args_tuple, kwargs_dict).
        """
        if not self.launch_args and not self.launch_kwargs:
            return self._default_task_args(model_instance)
        args = self._resolve_args(model_instance)
        kwargs = self._resolve_kwargs(model_instance)
        bound = self._build_bound_dict(args, kwargs)
        final_args = self._extract_final_args(bound)
        final_kwargs = self._extract_final_kwargs(bound)
        return final_args, final_kwargs

    def resolve_object_id_from_args(self, args, kwargs, model_label: str):
        """Public API: given a task call (args, kwargs), return the model instance id it refers to.

        Behavior depends on self.id_query (None, str, iterable, or dict).
        """
        if not self.id_query:
            return self._resolve_default_id(args)
        bound = self._build_bound_args(args, kwargs)
        if isinstance(self.id_query, dict):
            return self._resolve_dict_id(bound, model_label)
        if isinstance(self.id_query, (list, set, tuple)):
            return self._resolve_iterable_id(bound, model_label)
        if isinstance(self.id_query, str):
            return bound.get(self.id_query)
        return None

    def can_resolve_id_from_args(self, args, kwargs) -> bool:
        """Return True if resolving the model id from (args, kwargs) does not require a DB lookup."""
        if not self.id_query:
            return True

        bound = self._build_bound_args(args, kwargs)

        if isinstance(self.id_query, dict):
            keys = set(self.id_query.keys())
            if keys <= {"pk"}:
                mapped_arg_names = list(self.id_query.values())
                for name in mapped_arg_names:
                    if name in bound and bound.get(name) is not None:
                        return True
            return False

        if isinstance(self.id_query, (list, set, tuple)):
            if "pk" in self.id_query:
                for name in self.id_query:
                    if name in bound and bound.get(name) is not None:
                        return True
                return False
            return False
        if isinstance(self.id_query, str):
            return True

        return False

    # --- Default/simple helpers ---
    def _default_task_args(self, model_instance):
        pk = getattr(model_instance, "pk", None)
        return (pk,), {}

    def _resolve(self, spec, instance):
        if isinstance(spec, FieldReference):
            return spec.resolve(instance)
        return spec

    def _resolve_args(self, model_instance):
        return tuple(self._resolve(a, model_instance) for a in self.launch_args)

    def _resolve_kwargs(self, model_instance):
        return {k: self._resolve(v, model_instance) for k, v in self.launch_kwargs.items()}

    def _build_bound_dict(self, args, kwargs):
        bound: Dict[str, Any] = {}
        for i, v in enumerate(tuple(args)):
            if i < len(self._pos_names):
                bound[self._pos_names[i]] = v
            else:
                bound[f"arg{i}"] = v
        bound.update(kwargs or {})
        return bound

    def _extract_final_args(self, bound):
        return tuple(bound[n] for n in self._pos_names if n in bound)

    def _extract_final_kwargs(self, bound):
        return {k: v for k, v in bound.items() if k not in self._pos_names}

    # --- ID resolution helpers ---
    def _resolve_default_id(self, args):
        return args[0] if args else None

    def _build_bound_args(self, args, kwargs):
        bound: Dict[str, Any] = {}
        for i, v in enumerate(tuple(args)):
            if i < len(self._pos_names):
                bound[self._pos_names[i]] = v
            else:
                bound[f"arg{i}"] = v
        bound.update(dict(kwargs or {}))
        return bound

    def _resolve_dict_id(self, bound, model_label: str):
        if set(self.id_query.keys()) <= {"pk"}:
            for _, arg_name in self.id_query.items():
                if arg_name in bound:
                    return bound[arg_name]
            return None
        lookup = {m: bound.get(arg) for m, arg in self.id_query.items()}
        if any(v is None for v in lookup.values()):
            return None
        try:
            model_cls = apps.get_model(model_label)
            found = model_cls.objects.filter(**lookup).first()
            return getattr(found, "pk", None) if found else None
        except Exception:
            return None

    def _resolve_iterable_id(self, bound, model_label):
        if "pk" in self.id_query:
            for name in self.id_query:
                if name in bound and bound.get(name) is not None:
                    return bound.get(name)
            return None

        if any(name not in bound or bound.get(name) is None for name in self.id_query):
            return None
        try:
            model_cls = apps.get_model(model_label)
            lookup = {name: bound[name] for name in self.id_query}
            found = model_cls.objects.filter(**lookup).first()
            return getattr(found, "pk", None) if found else None
        except Exception:
            return None


class TaskRegistry:
    """
    Registry for managing task-to-model associations with dual lookup.

    Design decisions (feedback points 2-3):
    - _tasks uses str keys: Celery task names are unique strings, O(1) lookup
    - _models uses Type keys: Prevents collisions, type-safe, matches decorator input
    """

    def __init__(self):
        # task_name -> set of model labels
        self._tasks: Dict[str, set] = defaultdict(set)
        # model_label -> dict of task names (we use dict to preserve insertion order)
        self._models: Dict[str, Dict[str, None]] = defaultdict(dict)
        # model_labels of patched admin models
        self._patch_admin_models: Set[str] = set()
        # (task_name, model_label) -> TaskConfig
        self._task_configs: Dict[Tuple[str, str], Optional[TaskConfig]] = {}

    def register_task(
        self,
        model_class: Model,
        task_name: str,
        task_config: Optional[TaskConfig] = None,
    ) -> None:
        """Register a task for a model. Automatically patches Django admin."""
        model_label = model_class._meta.label
        self._tasks[task_name].add(model_label)
        self._models[model_label].setdefault(task_name, None)
        self._task_configs[(task_name, model_label)] = task_config
        self._patch_model_admin(model_class)

    def _patch_model_admin(self, model: Model) -> None:
        """Patch Django admin for a model."""

        model_label = model._meta.label
        model_name = model._meta.model_name
        model_app = model._meta.app_label

        # Avoid double patching
        if model_label in self._patch_admin_models:
            return

        # avoid patching non-registered models
        if model not in admin.site._registry:
            return

        admin_class = admin.site._registry[model]
        original_change_view = getattr(admin_class, "change_view", None)
        original_get_urls = getattr(admin_class, "get_urls", None)

        original_template = getattr(admin_class, "change_form_template", None)
        if not original_template:
            model_template = f"admin/{model._meta.app_label}/{model._meta.model_name}/change_form.html"
            try:
                get_template(model_template)
                original_template = model_template
            except TemplateDoesNotExist:
                original_template = "admin/change_form.html"

        admin_class.change_form_template = "admin/celery_task_tracker/change_form.html"

        def get_urls_with_tasktracker(admin_self):
            from django.urls import include, path

            urls = original_get_urls() if original_get_urls else ModelAdmin.get_urls(admin_self)

            custom_urls = [
                path(
                    "<path:object_id>/task-tracker/",
                    include(
                        "celery_task_tracker.urls",
                        namespace=f"task_tracker_{model_app}_{model_name}",
                    ),
                ),
            ]
            return custom_urls + urls

        def change_view_with_tasktracker(admin_self, request, object_id, form_url="", extra_context=None):
            if extra_context is None:
                extra_context = {}
            try:
                url = f"/admin/{model_app}/{model_name}/{object_id}/task-tracker/"
                extra_context["tasktracker_button_html"] = format_html(
                    '<a href="{}" class="btn btn-outline-primary form-control mt-3">'
                    '<i class="fas fa-tasks"></i> Task tracker</a>',
                    url,
                )
            except Exception:
                pass

            extra_context["original_change_form_template"] = original_template

            return (
                original_change_view(request, object_id, form_url, extra_context)
                if original_change_view
                else ModelAdmin.change_view(admin_self, request, object_id, form_url, extra_context)
            )

        admin_class.get_urls = types.MethodType(get_urls_with_tasktracker, admin_class)
        admin_class.change_view = types.MethodType(change_view_with_tasktracker, admin_class)

        self._patch_admin_models.add(model_label)

    def is_task_registered(self, task_name: str, model_class: Type) -> bool:
        """Check if task already registered for this model."""
        task = self._tasks.get(task_name)
        return bool(task and (model_class._meta.label in task))

    def get_models_for_task(self, task_name: str) -> Set[str]:
        """Get all model labels registered for a task."""
        return self._tasks.get(task_name, set())

    def get_tasks_for_model(self, model_label: str, include_hidden: bool = False) -> List[str]:
        """Get all task names registered for a model.

        Args:
            model_label: The model label to get tasks for.
            include_hidden: If True, include hidden tasks in the result.

        Returns:
            List of task names registered for the model.
        """
        model_tasks = self._models.get(model_label, {})
        if include_hidden:
            return list(model_tasks.keys())
        return [
            task_name
            for task_name in model_tasks.keys()
            if not (cfg := self._task_configs.get((task_name, model_label))) or not cfg.hidden
        ]

    def get_config_for_task(self, task_name: str, model_label: str) -> Optional[TaskConfig]:
        """Get task config for a specific (task_name, model_label) pair."""
        return self._task_configs.get((task_name, model_label))
