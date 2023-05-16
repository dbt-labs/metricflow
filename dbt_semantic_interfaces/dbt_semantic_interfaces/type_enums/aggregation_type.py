from dbt_semantic_interfaces.enum_extension import ExtendedEnum


class AggregationType(ExtendedEnum):
    """Aggregation methods for measures."""

    SUM = "sum"
    MIN = "min"
    MAX = "max"
    COUNT_DISTINCT = "count_distinct"
    SUM_BOOLEAN = "sum_boolean"
    AVERAGE = "average"
    PERCENTILE = "percentile"
    MEDIAN = "median"
    COUNT = "count"
