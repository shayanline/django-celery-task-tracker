import re

from django import template

register = template.Library()


def humanize_task_name(value):
    # Remove module prefix if present
    if "." in value:
        value = value.split(".")[-1]
    # Replace underscores with spaces
    value = value.replace("_", " ")
    # Convert camelCase or snake_case to words
    value = re.sub(r"(?<!^)(?=[A-Z])", " ", value)
    # Capitalize each word
    value = value.title()
    return value


register.filter("humanize_task_name", humanize_task_name)
register.filter("humanize_task_name", humanize_task_name)
