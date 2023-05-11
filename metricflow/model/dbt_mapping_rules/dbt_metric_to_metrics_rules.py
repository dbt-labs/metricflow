import traceback
from typing import List, Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricFilter, MetricNode
from metricflow.model.dbt_mapping_rules.dbt_mapping_rule import (
    DbtMappingRule,
    MappedObjects,
    assert_essential_metric_properties,
    get_and_assert_calc_method_mapping,
)
from dbt_semantic_interfaces.objects.metric import MetricInputMeasure, MetricInput, MetricType, MetricTypeParams
from dbt_semantic_interfaces.validations.validator_helpers import (
    ModelValidationResults,
    ValidationIssue,
    ValidationError,
)


class DbtToMetricName(DbtMappingRule):
    """Rule for mapping dbt metric names to metric names"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                objects.metrics[metric.name]["name"] = metric.name

            except Exception as e:
                issues.append(
                    ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMetricDescription(DbtMappingRule):
    """Rule for mapping dbt metric descriptions to metric descriptions"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                # metric descriptions are optional
                if metric.description:
                    assert_essential_metric_properties(metric=metric)
                    objects.metrics[metric.name]["description"] = metric.description

            except Exception as e:
                issues.append(
                    ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMetricType(DbtMappingRule):
    """Rule for mapping dbt metric calculation_methods to metric types"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                metric_type = get_and_assert_calc_method_mapping(dbt_metric=metric)
                objects.metrics[metric.name]["type"] = metric_type

            except Exception as e:
                issues.append(
                    ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMeasureProxyMetricTypeParams(DbtMappingRule):
    """Rule for mapping non-derived dbt metric names to metric measure inputs

    WARNING: This will clobber any other type_params for the metric
    """

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: d
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                assert_essential_metric_properties(metric=metric)
                # We only do this for MEASURE_PROXY metrics
                metric_type = get_and_assert_calc_method_mapping(dbt_metric=metric)
                if metric_type == MetricType.MEASURE_PROXY:
                    objects.metrics[metric.name]["type_params"] = MetricTypeParams(
                        measure=MetricInputMeasure(name=metric.name)
                    ).dict()

            except Exception as e:
                issues.append(
                    ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToDerivedMetricTypeParams(DbtMappingRule):
    """Rule for mapping derived dbt metric depends_on & expression to metric expression and metric inputs

    WARNING: This will clobber any other type_params for the metric
    TODO: We need to take another step to modify the dbt expression to an appropriate MetricFlow expression
    """

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                metric_type = get_and_assert_calc_method_mapping(dbt_metric=metric)
                if metric_type == MetricType.DERIVED:
                    assert metric.expression, f"Expected an `expression` for `{metric.name}` metric, got `None`"
                    assert (
                        metric.depends_on is not None and len(metric.depends_on) > 0
                    ), f"Expected a non-empty list for `depends_on` for `{metric.name}` metric, got `{metric.depends_on}`"

                    # `depends_on` is a list of `database.schema.name` strings, we only want the `name` part
                    sub_metrics = [name.split(".")[2] for name in metric.depends_on]
                    metric_inputs = [MetricInput(name=name) for name in sub_metrics]

                    objects.metrics[metric.name]["type_params"] = MetricTypeParams(
                        expr=metric.expression,  # TODO <- Need to make this a MetricFlow expression first
                        metrics=metric_inputs,
                    ).dict()

            except Exception as e:
                issues.append(
                    ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtToMetricConstraint(DbtMappingRule):
    """Rule for mapping dbt metric filters to metric constraint

    With this rule we just build the SQL where constraint from the filters as
    we'd define it in the YAML definition of a MetricFlow metric. This makes it
    so that when we parse the mapped objects to MetricFlow model elements, the
    constraint goes through the WhereClauseConstraint parser.
    """

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            try:
                # Filters are optional
                if metric.filters:
                    assert_essential_metric_properties(metric=metric)
                    filters: List[MetricFilter] = metric.filters
                    clauses = [f"{filter.field} {filter.operator} {filter.value}" for filter in filters]
                    objects.metrics[metric.name]["constraint"] = " AND ".join(clauses)
            except Exception as e:
                issues.append(
                    ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                )

        return ModelValidationResults.from_issues_sequence(issues=issues)
