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
from metricflow.model.dbt_mapping_rules.dbt_metric_model_to_semantic_model_rules import (
    DbtMapToSemanticModelName,
    DbtMapToSemanticModelDescription,
    DbtMapSemanticModelNodeRelation,
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
from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.metric import Metric
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.parsing.dir_to_model import ModelBuildResult
from dbt_semantic_interfaces.validations.validator_helpers import (
    ModelValidationResults,
    ValidationError,
    ValidationIssue,
)

logger = logging.getLogger(__name__)

DEFAULT_RULES: FrozenSet[DbtMappingRule] = frozenset(
    [
        # Build semantic models
        DbtMapToSemanticModelName(),
        DbtMapToSemanticModelDescription(),
        DbtMapSemanticModelNodeRelation(),
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
        semantic_model_class: Type[SemanticModel] = SemanticModel,
        metric_class: Type[Metric] = Metric,
    ) -> None:
        """Initializer for DbtConverter class

        Args:
            rules: A collection of DbtMappingRules which get saved as a FrozenSet (immutable and unordered). Defaults to DEFAULT_RULES.
            semantic_model_class: SemanticModel class to parse the mapped semantic models to. Defaults to MetricFlow SemanticModel class.
            metric_class: Metric class to parse the mapped metrics to. Defaults to MetricFlow Metric class.
        """
        self._unordered_rules = frozenset(rules)
        self.semantic_model_class = semantic_model_class
        self.metric_class = metric_class

    def _map_dbt_to_metricflow(self, dbt_metrics: Tuple[MetricNode, ...]) -> DbtMappingResults:
        """Using a series of rules transforms dbt metrics into a mapped dict representing SemanticManifest objects"""
        mapped_objects = MappedObjects()
        validation_results = ModelValidationResults()

        for rule in self._unordered_rules:
            mapping_rule_issues = rule.run(dbt_metrics=dbt_metrics, objects=mapped_objects)
            validation_results = ModelValidationResults.merge([validation_results, mapping_rule_issues])

        return DbtMappingResults(mapped_objects=mapped_objects, validation_results=validation_results)

    def _build_metricflow_model(self, mapped_objects: MappedObjects) -> ModelBuildResult:
        """Takes in a map of dicts representing SemanticManifest objects, and builds a SemanticManifest"""
        # we don't want to modify the passed in objects, so we decopy them
        copied_objects = deepcopy(mapped_objects)

        # Move dimensions, entities, and measures on to their respective semantic models
        for semantic_model_name, dimensions_map in copied_objects.dimensions.items():
            copied_objects.semantic_models[semantic_model_name]["dimensions"] = list(dimensions_map.values())
        for semantic_model_name, entities_map in copied_objects.entities.items():
            copied_objects.semantic_models[semantic_model_name]["entities"] = list(entities_map.values())
        for semantic_model_name, measure_map in copied_objects.measures.items():
            copied_objects.semantic_models[semantic_model_name]["measures"] = list(measure_map.values())

        issues: List[ValidationIssue] = []

        semantic_models: List[Type[SemanticModel]] = []
        for semantic_model_dict in copied_objects.semantic_models.values():
            try:
                semantic_models.append(self.semantic_model_class.parse_obj(semantic_model_dict))
            except Exception as e:
                issues.append(
                    ValidationError(
                        message=f"Failed to parse dict of semantic model {semantic_model_dict.get('name')} to object",
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
            model=SemanticManifest(semantic_models=semantic_models, metrics=metrics),
            issues=ModelValidationResults.from_issues_sequence(issues=issues),
        )

    def convert(self, dbt_metrics: Tuple[MetricNode, ...]) -> ModelBuildResult:
        """Builds a SemanticManifest from dbt MetricNodes"""
        mapping_result = self._map_dbt_to_metricflow(dbt_metrics=dbt_metrics)

        if mapping_result.validation_results.has_blocking_issues:
            return ModelBuildResult(
                model=SemanticManifest(semantic_models=[], metrics=[]), issues=mapping_result.validation_results
            )

        build_result = self._build_metricflow_model(mapped_objects=mapping_result.mapped_objects)
        return ModelBuildResult(
            build_result.model,
            ModelValidationResults.merge([mapping_result.validation_results, build_result.issues]),
        )
