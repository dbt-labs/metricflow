import traceback
from typing import List, Tuple

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.dataflow.sql_table import SqlTable
from metricflow.model.dbt_mapping_rules.dbt_mapping_rule import (
    DbtMappingRule,
    MappedObjects,
    assert_metric_model_name,
)
from metricflow.model.validations.validator_helpers import ModelValidationResults, ValidationIssue, ValidationError


class DbtMapToDataSourceName(DbtMappingRule):
    """Rule for mapping dbt metric model names to data source names"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            # Derived metrics don't have models, so skip when model doesn't exist
            if metric.model:
                try:
                    assert_metric_model_name(metric=metric)
                    objects.data_sources[metric.model.name]["name"] = metric.model.name

                except Exception as e:
                    issues.append(
                        ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                    )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtMapToDataSourceDescription(DbtMappingRule):
    """Rule for mapping dbt metric model descriptions to data source descriptions"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            # Derived metrics don't have models, so skip when model doesn't exist
            if metric.model:
                try:
                    assert_metric_model_name(metric=metric)
                    # Don't need to assert `metric.model.description` because
                    # it's optional and can be set to None
                    objects.data_sources[metric.model.name]["description"] = metric.model.description

                except Exception as e:
                    issues.append(
                        ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                    )

        return ModelValidationResults.from_issues_sequence(issues=issues)


class DbtMapDataSourceSqlTable(DbtMappingRule):
    """Rule for mapping dbt metric models to data source sql tables"""

    @staticmethod
    def run(dbt_metrics: Tuple[MetricNode, ...], objects: MappedObjects) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for metric in dbt_metrics:
            # Derived metrics don't have models, so skip when model doesn't exist
            if metric.model:
                try:
                    assert_metric_model_name(metric=metric)
                    assert (
                        metric.model.database
                    ), f"Expected a `database` for `{metric.name}` metric's `model`, got `None`"
                    assert metric.model.schema, f"Expected a `schema` for `{metric.name}` metric's `model`, got `None`"
                    objects.data_sources[metric.model.name]["sql_table"] = SqlTable(
                        db_name=metric.model.database,
                        schema_name=metric.model.schema,
                        table_name=metric.model.name,
                    ).sql

                except Exception as e:
                    issues.append(
                        ValidationError(message=str(e), extra_detail="".join(traceback.format_tb(e.__traceback__)))
                    )

        return ModelValidationResults.from_issues_sequence(issues=issues)
