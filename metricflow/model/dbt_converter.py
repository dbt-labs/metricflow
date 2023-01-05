from copy import deepcopy
import logging
import traceback
from typing import Collection, FrozenSet, List, Tuple, Type

from dbt_metadata_client.dbt_metadata_api_schema import MetricNode
from metricflow.model.dbt_mapping_rules.dbt_mapping_rule import (
    DbtMappingRule,
    DbtMappingResults,
    MappedObjects,
)
from metricflow.model.dbt_mapping_rules.dbt_metric_model_to_data_source_rules import (
    DbtMapToDataSourceName,
    DbtMapToDataSourceDescription,
    DbtMapDataSourceSqlTable,
)
from metricflow.model.dbt_mapping_rules.dbt_metric_to_metrics_rules import (
    DbtToMetricName,
    DbtToMetricDescription,
    DbtToMetricType,
    DbtToMeasureProxyMetricTypeParams,
    DbtToMetricConstraint,
    DbtToDerivedMetricTypeParams,
)
from metricflow.model.dbt_mapping_rules.dbt_metric_to_dimensions_rules import (
    DbtDimensionsToDimensions,
    DbtTimestampToDimension,
    DbtFiltersToDimensions,
)
from metricflow.model.dbt_mapping_rules.dbt_metric_to_measure import (
    DbtToMeasureName,
    DbtToMeasureExpr,
    DbtToMeasureAgg,
    DbtToMeasureAggTimeDimension,
)
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.materialization import Materialization
from metricflow.model.objects.metric import Metric
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import ModelBuildResult
from metricflow.model.validations.validator_helpers import ModelValidationResults, ValidationError, ValidationIssue

logger = logging.getLogger(__name__)

DEFAULT_RULES: FrozenSet[DbtMappingRule] = frozenset(
    [
        # Build data sources
        DbtMapToDataSourceName(),
        DbtMapToDataSourceDescription(),
        DbtMapDataSourceSqlTable(),
        # Build Metrics
        DbtToMetricName(),
        DbtToMetricDescription(),
        DbtToMetricType(),
        DbtToMeasureProxyMetricTypeParams(),
        DbtToMetricConstraint(),
        DbtToDerivedMetricTypeParams(),
        # Build Dimensions
        DbtDimensionsToDimensions(),
        DbtTimestampToDimension(),
        DbtFiltersToDimensions(),
        # Build Measures
        DbtToMeasureName(),
        DbtToMeasureExpr(),
        DbtToMeasureAgg(),
        DbtToMeasureAggTimeDimension(),
    ]
)


class DbtConverter:
    """Handles converting a list of dbt MetricNodes into a MetricFlow Model

    A DbtConverter is a tool for converting dbt metrics into a MetricFlow Model.
    It does so by operating DbtMappingRules which map dbt node properties to
    MetricFlow Model properties. Once a DbtConverter is instantiated, the rules
    for that DbtConverter are immutable. Additionally, rules are stored
    UNORDERED. For clarity, the order of the rules is not guaranteed, and should
    not be depended on. Any shuffling of a given set of rules should produce the
    same MetricFlow model.
    """

    def __init__(
        self,
        rules: Collection[DbtMappingRule] = DEFAULT_RULES,
        data_source_class: Type[DataSource] = DataSource,
        metric_class: Type[Metric] = Metric,
        materialization_class: Type[Materialization] = Materialization,
    ) -> None:
        """Initializer for DbtConverter class

        Args:
            rules: A collection of DbtMappingRules which get saved as a FrozenSet (immutable and unordered). Defaults to DEFAULT_RULES.
            data_source_class: DataSource class to parse the mapped data sources to. Defaults to MetricFlow DataSource class.
            metric_class: Metric class to parse the mapped metrics to. Defaults to MetricFlow Metric class.
            materialization_class: Materialization class to parse the mapped materializations to. Defaults to MetricFlow Materialization class.
        """
        self._unordered_rules = frozenset(rules)
        self.data_source_class = data_source_class
        self.metric_class = metric_class
        self.materialization_class = materialization_class

    def _map_dbt_to_metricflow(self, dbt_metrics: Tuple[MetricNode, ...]) -> DbtMappingResults:
        """Using a series of rules transforms dbt metrics into a mapped dict representing UserConfiguredModel objects"""
        mapped_objects = MappedObjects()
        validation_results = ModelValidationResults()

        for rule in self._unordered_rules:
            mapping_rule_issues = rule.run(dbt_metrics=dbt_metrics, objects=mapped_objects)
            validation_results = ModelValidationResults.merge([validation_results, mapping_rule_issues])

        return DbtMappingResults(mapped_objects=mapped_objects, validation_results=validation_results)

    def _build_metricflow_model(self, mapped_objects: MappedObjects) -> ModelBuildResult:
        """Takes in a map of dicts representing UserConfiguredModel objects, and builds a UserConfiguredModel"""
        # we don't want to modify the passed in objects, so we decopy them
        copied_objects = deepcopy(mapped_objects)

        # Move dimensions, identifiers, and measures on to their respective data sources
        for data_source_name, dimensions_map in copied_objects.dimensions.items():
            copied_objects.data_sources[data_source_name]["dimensions"] = list(dimensions_map.values())
        for data_source_name, identifiers_map in copied_objects.identifiers.items():
            copied_objects.data_sources[data_source_name]["identifiers"] = list(identifiers_map.values())
        for data_source_name, measure_map in copied_objects.measures.items():
            copied_objects.data_sources[data_source_name]["measures"] = list(measure_map.values())

        issues: List[ValidationIssue] = []

        data_sources: List[Type[DataSource]] = []
        for data_source_dict in copied_objects.data_sources.values():
            try:
                data_sources.append(self.data_source_class.parse_obj(data_source_dict))
            except Exception as e:
                issues.append(
                    ValidationError(
                        message=f"Failed to parse dict of data source {data_source_dict.get('name')} to object",
                        extra_detail="".join(traceback.format_tb(e.__traceback__)),
                    )
                )

        materializations: List[Type[Materialization]] = []
        for materialization_dict in copied_objects.materializations.values():
            try:
                materializations.append(self.materialization_class.parse_obj(materialization_dict))
            except Exception as e:
                issues.append(
                    ValidationError(
                        message=f"Failed to parse dict of materialization {materialization_dict.get('name')} to object",
                        extra_detail="".join(traceback.format_tb(e.__traceback__)),
                    )
                )

        metrics: List[Type[Metric]] = []
        for metric_dict in copied_objects.metrics.values():
            try:
                metrics.append(self.metric_class.parse_obj(metric_dict))
            except Exception as e:
                issues.append(
                    ValidationError(
                        message=f"Failed to parse dict of metric {metric_dict.get('name')} to object",
                        extra_detail="".join(traceback.format_tb(e.__traceback__)),
                    )
                )

        return ModelBuildResult(
            model=UserConfiguredModel(data_sources=data_sources, materializations=materializations, metrics=metrics),
            issues=ModelValidationResults.from_issues_sequence(issues=issues),
        )

    def convert(self, dbt_metrics: Tuple[MetricNode, ...]) -> ModelBuildResult:
        """Builds a UserConfiguredModel from dbt MetricNodes"""
        mapping_result = self._map_dbt_to_metricflow(dbt_metrics=dbt_metrics)

        if mapping_result.validation_results.has_blocking_issues:
            return ModelBuildResult(
                model=UserConfiguredModel(data_sources=[], metrics=[]), issues=mapping_result.validation_results
            )

        build_result = self._build_metricflow_model(mapped_objects=mapping_result.mapped_objects)
        return ModelBuildResult(
            build_result.model,
            ModelValidationResults.merge([mapping_result.validation_results, build_result.issues]),
        )
