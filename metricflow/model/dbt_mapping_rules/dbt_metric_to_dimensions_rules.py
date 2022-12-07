import traceback
from typing import Dict, List, Optional, Tuple

from dateutil.parser import parse, ParserError
from dbt_metadata_client.dbt_metadata_api_schema import CatalogColumn, MetricNode, MetricFilter
from metricflow.model.dbt_mapping_rules.dbt_mapping_rule import (
    DbtMappingRule,
    MappedObjects,
    assert_metric_model_name,
)
from metricflow.model.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from metricflow.time.time_granularity import TimeGranularity
from metricflow.model.validations.validator_helpers import ModelValidationResults, ValidationIssue, ValidationError


DBT_COLUMN_TYPES_TO_DIMENSION_TYPES: Dict[str, DimensionType] = {
    "DATE": DimensionType.TIME,
    "TIMESTAMP_TZ": DimensionType.TIME,
    "TIMESTAMP_NTZ": DimensionType.TIME,
    "TEXT": DimensionType.CATEGORICAL,
    "BOOLEAN": DimensionType.CATEGORICAL,
    "NUMBER": DimensionType.CATEGORICAL,  # Measure?
    "FLOAT": DimensionType.CATEGORICAL,  # Measure?
}


def dimension_for_dimension_in_columns(dimension_name: str, columns: List[CatalogColumn]) -> Optional[Dimension]:
    """This function tries to build the dimension for a given dimension name in relation to the list of dbt CatalogColumns"""
    # We uppercase the name because dbt stores the column names upper'd
    uppered_name = dimension_name.upper()

    for column in columns:
        if uppered_name == column.name:
            dim_type = DBT_COLUMN_TYPES_TO_DIMENSION_TYPES[column.type]
            if dim_type == DimensionType.CATEGORICAL:
                return Dimension(
                    name=dimension_name,
                    type=DimensionType.CATEGORICAL,
                )
            else:
                return Dimension(
                    name=dimension_name,
                    type=DimensionType.TIME,
                    type_params=DimensionTypeParams(is_primary=False, time_granularity=TimeGranularity.DAY),
                )

    return None


class DbtDimensionsToDimensions(DbtMappingRule):
    """Rule for mapping dbt metric dimensions to data source dimensions"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            # Skip metrics which don't have dimensions or a model to attach them to
            if metric.dimensions and len(metric.dimensions) > 0 and metric.model:
                try:
                    assert_metric_model_name(metric=metric)
                    for dimension in metric.dimensions:
                        built_dimension = dimension_for_dimension_in_columns(dimension, metric.model.columns)
                        if built_dimension is not None:
                            objects.dimensions[metric.model.name][built_dimension.name] = built_dimension.dict()
                        else:
                            issues.append(
                                ValidationError(
                                    message=f"Dimension `{dimension}` was not found in the dbt model's columns",
                                    extra_detail=f"columns: {','.join([column.name for column in metric.model.columns])}",
                                )
                            )

                except Exception as e:
                    issues.append(
                        ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                    )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtTimestampToDimension(DbtMappingRule):
    """Rule for mapping dbt metric timestamps to data source dimensions"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            # Creating dimensions only matters if there is a data source (model) to attach them too
            if metric.model:
                try:
                    assert_metric_model_name(metric=metric)
                    assert (
                        metric.timestamp is not None
                    ), f"Expected a value for `{metric.name}` metric's `timestamp`, got `None`"
                    objects.dimensions[metric.model.name][metric.timestamp] = Dimension(
                        name=metric.timestamp,
                        type=DimensionType.TIME,
                        type_params=DimensionTypeParams(is_primary=False, time_granularity=TimeGranularity.DAY),
                    ).dict()

                except Exception as e:
                    issues.append(
                        ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                    )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtFiltersToDimensions(DbtMappingRule):
    """Rule for mapping dbt metric filters to data source dimensions"""

    @staticmethod
    def _filter_value_is_timestamp(filter_value: str) -> bool:
        """Determines if a given dbt MetricFilter represents a TIME type dimension or not

        Every dbt MetricFilter represents a dimension. However, we aren't given
        direct information if should be a CATEGORICAL or TIME dimension. The
        good news is that every filter includes a `value` to compare with. Thus
        we can check if this is iso datetime parseable to see if it should be
        considered a TIME type dimension
        """
        # filter values which represent date/time of some sort must start
        # and end with single quotes
        if filter_value.startswith("'") and filter_value.endswith("'"):
            # remove the starting and ending '
            potential_timestamp = filter_value.strip("'")
            # dateutil.parser.parse will raise a ParseError if it can't parse the string
            try:
                parse(potential_timestamp)
                return True
            except ParserError:
                return False
        else:
            return False

    @staticmethod
    def _dimension_from_filter(filter: MetricFilter) -> Dimension:
        """Builds a dimension from a dbt MetricFilter assigning the appropriate type"""
        if DbtFiltersToDimensions._filter_value_is_timestamp(filter.value):
            return Dimension(
                name=filter.field,
                type=DimensionType.TIME,
                type_params=DimensionTypeParams(is_primary=False, time_granularity=TimeGranularity.DAY),
            )
        else:
            return Dimension(
                name=filter.field,
                type=DimensionType.CATEGORICAL,
            )

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            # Skip if a metric doesn't have filters or a model
            if metric.filters and metric.model:
                try:
                    filters: List[MetricFilter] = metric.filters
                    for filter in filters:
                        assert_metric_model_name(metric=metric)
                        dimension = DbtFiltersToDimensions._dimension_from_filter(filter=filter)
                        objects.dimensions[metric.model.name][filter.field] = dimension.dict()

                except Exception as e:
                    issues.append(
                        ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                    )

        return ModelValidationResults.from_issues_sequence(issues=issues)
