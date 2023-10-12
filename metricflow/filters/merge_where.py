from __future__ import annotations

from typing import Optional

from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.protocols import WhereFilter, WhereFilterIntersection


def merge_to_single_where_filter(where_filter_intersection: WhereFilterIntersection) -> Optional[WhereFilter]:
    """Returns a single where filter that is equivalent to the given intersection."""
    if len(where_filter_intersection.where_filters) == 0:
        return None

    if len(where_filter_intersection.where_filters) == 1:
        return where_filter_intersection.where_filters[0]

    each_where_filter_condition = [
        "( " + where_filter.where_sql_template + " )" for where_filter in where_filter_intersection.where_filters
    ]

    return PydanticWhereFilter(where_sql_template=" AND ".join(each_where_filter_condition))
