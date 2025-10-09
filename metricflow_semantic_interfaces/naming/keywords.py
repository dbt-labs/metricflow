# A double underscore used as a seperator in group by item names.
# e.g. user__country
DUNDER = "__"

# The name for the time dimension used to tabulate / plot metrics.
METRIC_TIME_ELEMENT_NAME = "metric_time"


def is_metric_time_name(element_name: str) -> bool:
    """Returns True if the given element name corresponds to metric time."""
    return element_name == METRIC_TIME_ELEMENT_NAME
