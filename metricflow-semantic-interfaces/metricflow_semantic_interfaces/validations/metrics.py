from __future__ import annotations

from typing import Dict, Generic, List, Literal, Optional, Sequence, Set, Tuple, Union

from metricflow_semantic_interfaces.implementations.metric import PydanticMetric
from metricflow_semantic_interfaces.protocols import (
    ConversionTypeParams,
    Dimension,
    Metric,
    MetricInputMeasure,
    MetricTimeWindow,
    SemanticManifest,
    SemanticManifestT,
    SemanticModel,
)
from metricflow_semantic_interfaces.protocols.metadata import Metadata
from metricflow_semantic_interfaces.protocols.metric import MetricInput
from metricflow_semantic_interfaces.protocols.where_filter import WhereFilterIntersection
from metricflow_semantic_interfaces.references import (
    DimensionReference,
    MeasureReference,
    MetricModelReference,
    MetricReference,
)
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    MetricType,
    TimeGranularity,
)
from metricflow_semantic_interfaces.validations.shared_measure_and_metric_helpers import (
    SharedMeasureAndMetricHelpers,
)
from metricflow_semantic_interfaces.validations.unique_valid_name import UniqueAndValidNameRule
from metricflow_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    MetricContext,
    SemanticManifestValidationRule,
    ValidationError,
    ValidationIssue,
    ValidationWarning,
    validate_safely,
)

# Avoids breaking change from moving this class out of this file.
from metricflow_semantic_interfaces.validations.where_filters import (
    WhereFiltersAreParseable,  # noQa
)

TEMP_CUSTOM_GRAIN_MSG = "Custom granularities are not supported for this field yet."


class MetricValidationRuleHelpers:
    """Helpers for metric validation rules."""

    @staticmethod
    def get_metric_from_manifest(metric_name: str, semantic_manifest: SemanticManifest) -> Optional[Metric]:
        """Get a metric from the manifest by name."""
        return next((metric for metric in semantic_manifest.metrics if metric.name == metric_name), None)

    # TODO add a function for default context.


class CumulativeMetricRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that cumulative metrics are configured properly."""

    @classmethod
    def _validate_input_measure_xor_metric(cls, metric: Metric) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        input_metric = (
            metric.type_params.cumulative_type_params.metric if metric.type_params.cumulative_type_params else None
        )
        if metric.type_params.measure is not None and input_metric is not None:
            issues.append(
                ValidationWarning(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric.metadata),
                        metric=MetricModelReference(metric_name=metric.name),
                    ),
                    message=f"Cumulative metric '{metric.name}' should not have both a measure and a metric as "
                    "inputs. The measure will be ignored; please remove it to avoid confusion.",
                )
            )
        elif metric.type_params.measure is None and input_metric is None:
            issues.append(
                ValidationWarning(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric.metadata),
                        metric=MetricModelReference(metric_name=metric.name),
                    ),
                    message=f"Cumulative metric '{metric.name}' must have either a measure or a metric as inputs. "
                    "Please add one of them.",
                )
            )
        return issues

    @classmethod
    @validate_safely(whats_being_done="running model validation ensuring cumulative metrics are valid")
    def validate_manifest(cls, semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []

        custom_granularity_names = {
            granularity.name
            for time_spine in semantic_manifest.project_configuration.time_spines
            for granularity in time_spine.custom_granularities
        }
        standard_granularities = {item.value.lower() for item in TimeGranularity}

        for metric in semantic_manifest.metrics or []:
            if metric.type != MetricType.CUMULATIVE:
                continue

            issues.extend(cls._validate_input_measure_xor_metric(metric=metric))

            metric_context = MetricContext(
                file_context=FileContext.from_metadata(metadata=metric.metadata),
                metric=MetricModelReference(metric_name=metric.name),
            )

            for field in ("window", "grain_to_date"):
                type_params_field_value = getattr(metric.type_params, field)

                # Warn that window or grain_to_date is mismatched across params.
                cumulative_type_params_field_value = (
                    getattr(metric.type_params.cumulative_type_params, field)
                    if metric.type_params.cumulative_type_params
                    else None
                )
                if (
                    field == "window"
                    and type_params_field_value
                    and cumulative_type_params_field_value
                    and cumulative_type_params_field_value != type_params_field_value
                ):
                    issues.append(
                        ValidationError(
                            context=metric_context,
                            message=(
                                f"Got differing values for `{field}` on cumulative metric '{metric.name}'. In "
                                f"`type_params.{field}`, got '{type_params_field_value}'. In "
                                f"`type_params.cumulative_type_params.{field}`, got "
                                f"'{cumulative_type_params_field_value}'. Please remove the value from "
                                f"`type_params.{field}`."
                            ),
                        )
                    )

            window = metric.type_params.window
            if metric.type_params.cumulative_type_params and metric.type_params.cumulative_type_params.window:
                window = metric.type_params.cumulative_type_params.window
            grain_to_date = metric.type_params.grain_to_date.value if metric.type_params.grain_to_date else None
            if metric.type_params.cumulative_type_params and metric.type_params.cumulative_type_params.grain_to_date:
                grain_to_date = metric.type_params.cumulative_type_params.grain_to_date

            if grain_to_date and grain_to_date not in standard_granularities:
                issues.append(
                    ValidationError(
                        context=metric_context,
                        message=(
                            f"Invalid time granularity found in `grain_to_date`: '{grain_to_date}'. "
                            f"{TEMP_CUSTOM_GRAIN_MSG}"
                        ),
                    )
                )

            if window and grain_to_date:
                issues.append(
                    ValidationError(
                        context=metric_context,
                        message="Both window and grain_to_date set for cumulative metric. Please set one or the other.",
                    )
                )

            if window:
                issues.extend(
                    cls.validate_metric_time_window(
                        metric_context=metric_context, window=window, custom_granularities=custom_granularity_names
                    )
                )

        return issues

    @classmethod
    def validate_metric_time_window(  # noqa: D102
        cls,
        metric_context: MetricContext,
        window: MetricTimeWindow,
        custom_granularities: Set[str],
        allow_custom: bool = False,
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        standard_granularities = {item.value.lower() for item in TimeGranularity}
        valid_granularities = custom_granularities | standard_granularities
        window_granularity = window.granularity
        if window_granularity.endswith("s") and window_granularity[:-1] in valid_granularities:
            # months -> month
            window_granularity = window_granularity[:-1]

        msg = f"Invalid time granularity '{window_granularity}' in window: '{window.window_string}'"
        if window_granularity not in valid_granularities:
            issues.append(
                ValidationError(
                    context=metric_context,
                    message=msg,
                )
            )
        elif not allow_custom and (window_granularity not in standard_granularities):
            issues.append(
                ValidationError(
                    context=metric_context,
                    message=msg + " " + TEMP_CUSTOM_GRAIN_MSG,
                )
            )

        return issues


class DerivedMetricRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that derived metrics are configured properly."""

    @staticmethod
    @validate_safely(whats_being_done="checking that the alias set are not unique and distinct")
    def _validate_alias_collision(metric: Metric) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        if metric.type == MetricType.DERIVED:
            metric_context = MetricContext(
                file_context=FileContext.from_metadata(metadata=metric.metadata),
                metric=MetricModelReference(metric_name=metric.name),
            )
            input_metrics = metric.type_params.metrics or []
            used_names = {input_metric.name for input_metric in input_metrics}
            for input_metric in input_metrics:
                if input_metric.alias:
                    issues += UniqueAndValidNameRule.check_valid_name(input_metric.alias, metric_context)
                    if input_metric.alias in used_names:
                        issues.append(
                            ValidationError(
                                context=metric_context,
                                message=f"Alias '{input_metric.alias}' for input metric: '{input_metric.name}' is "
                                "already being used. Please choose another alias.",
                            )
                        )
                        used_names.add(input_metric.alias)
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that the input metrics exist")
    def _validate_input_metrics_exist(semantic_manifest: SemanticManifest) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        all_metrics = {m.name for m in semantic_manifest.metrics}
        for metric in semantic_manifest.metrics:
            metric_context = MetricContext(
                file_context=FileContext.from_metadata(metadata=metric.metadata),
                metric=MetricModelReference(metric_name=metric.name),
            )
            if metric.type == MetricType.DERIVED:
                if not metric.type_params.metrics:
                    issues.append(
                        ValidationError(
                            context=metric_context,
                            message=f"No input metrics found for derived metric '{metric.name}'. "
                            "Please add metrics to type_params.metrics.",
                        )
                    )
                for input_metric in metric.type_params.metrics or []:
                    if input_metric.name not in all_metrics:
                        issues.append(
                            ValidationError(
                                context=metric_context,
                                message=f"For metric: {metric.name}, input metric: '{input_metric.name}' does not "
                                "exist as a configured metric in the model.",
                            )
                        )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that input metric time offset params are valid")
    def _validate_time_offset_params(metric: Metric, custom_granularities: Set[str]) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        standard_granularities = {item.value.lower() for item in TimeGranularity}

        metric_context = MetricContext(
            file_context=FileContext.from_metadata(metadata=metric.metadata),
            metric=MetricModelReference(metric_name=metric.name),
        )
        for input_metric in metric.type_params.metrics or []:
            if input_metric.offset_window:
                issues += CumulativeMetricRule.validate_metric_time_window(
                    metric_context=metric_context,
                    window=input_metric.offset_window,
                    custom_granularities=custom_granularities,
                    allow_custom=True,
                )
            if input_metric.offset_to_grain and input_metric.offset_to_grain not in standard_granularities:
                issues.append(
                    ValidationError(
                        context=metric_context,
                        message=(
                            f"Invalid time granularity found in `offset_to_grain`: '{input_metric.offset_to_grain}'. "
                            f"{TEMP_CUSTOM_GRAIN_MSG}"
                        ),
                    )
                )
            if input_metric.offset_window and input_metric.offset_to_grain:
                issues.append(
                    ValidationError(
                        context=metric_context,
                        message=f"Both offset_window and offset_to_grain set for derived metric '{metric.name}' on "
                        f"input metric '{input_metric.name}'. Please set one or the other.",
                    )
                )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that the expr field uses the input metrics")
    def _validate_expr(metric: Metric) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        if metric.type == MetricType.DERIVED:
            if not metric.type_params.expr:
                issues.append(
                    ValidationWarning(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=metric.metadata),
                            metric=MetricModelReference(metric_name=metric.name),
                        ),
                        message=f"No `expr` set for derived metric {metric.name}. "
                        "Please add an `expr` that references all input metrics.",
                    )
                )
            else:
                for input_metric in metric.type_params.metrics or []:
                    name = input_metric.alias or input_metric.name
                    if name not in metric.type_params.expr:
                        issues.append(
                            ValidationWarning(
                                context=MetricContext(
                                    file_context=FileContext.from_metadata(metadata=metric.metadata),
                                    metric=MetricModelReference(metric_name=metric.name),
                                ),
                                message=f"Input metric '{name}' is not used in `expr`: '{metric.type_params.expr}' for "
                                f"derived metric '{metric.name}'. Please update the `expr` or remove the input metric.",
                            )
                        )

        return issues

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring derived metrics properties are configured properly"
    )
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []

        custom_granularity_names = {
            granularity.name
            for time_spine in semantic_manifest.project_configuration.time_spines
            for granularity in time_spine.custom_granularities
        }

        issues += DerivedMetricRule._validate_input_metrics_exist(semantic_manifest=semantic_manifest)
        for metric in semantic_manifest.metrics or []:
            issues += DerivedMetricRule._validate_alias_collision(metric=metric)
            issues += DerivedMetricRule._validate_time_offset_params(
                metric=metric, custom_granularities=custom_granularity_names
            )
            issues += DerivedMetricRule._validate_expr(metric=metric)
        return issues


class ConversionMetricRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that conversion metrics are configured properly."""

    @staticmethod
    def _validate_measure_xor_metric_for_each_input(metric: Metric) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        if metric.type_params.conversion_type_params is None:
            return issues

        conversion_type_params = PydanticMetric.get_checked_conversion_type_params(metric=metric)
        base_measure = conversion_type_params.base_measure
        base_metric = conversion_type_params.base_metric
        if base_measure is not None and base_metric is not None:
            issues.append(
                ValidationWarning(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric.metadata),
                        metric=MetricModelReference(metric_name=metric.name),
                    ),
                    message=f"Conversion metric '{metric.name}' should not have both a base measure "
                    "and a base metric as inputs. The base measure will be ignored; please "
                    "remove it to avoid confusion.",
                )
            )
        elif base_measure is None and base_metric is None:
            issues.append(
                ValidationError(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric.metadata),
                        metric=MetricModelReference(metric_name=metric.name),
                    ),
                    message=f"Conversion metric '{metric.name}' must have either a base measure or a base metric "
                    "as inputs. Please add one of them.",
                )
            )

        conversion_measure = metric.type_params.conversion_type_params.conversion_measure
        conversion_metric = metric.type_params.conversion_type_params.conversion_metric
        if conversion_measure is not None and conversion_metric is not None:
            issues.append(
                ValidationWarning(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric.metadata),
                        metric=MetricModelReference(metric_name=metric.name),
                    ),
                    message=f"Conversion metric '{metric.name}' should not have both a conversion measure "
                    "and a conversion metric as inputs. The conversion measure will be ignored; please "
                    "remove it to avoid confusion.",
                )
            )
        elif conversion_measure is None and conversion_metric is None:
            issues.append(
                ValidationError(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric.metadata),
                        metric=MetricModelReference(metric_name=metric.name),
                    ),
                    message="Conversion metric '{metric.name}' must have either a conversion measure or "
                    "a conversion metric as inputs. Please add one of them.",
                )
            )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking that the params of metric are valid if it is a conversion metric")
    def _validate_type_params(
        metric: Metric, conversion_type_params: ConversionTypeParams, custom_granularity_names: Set[str]
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        window = conversion_type_params.window
        if window:
            issues += CumulativeMetricRule.validate_metric_time_window(
                metric_context=MetricContext(
                    file_context=FileContext.from_metadata(metadata=metric.metadata),
                    metric=MetricModelReference(metric_name=metric.name),
                ),
                window=window,
                custom_granularities=custom_granularity_names,
            )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="checks that the entity exists in the base/conversion semantic model")
    def _validate_entity_exists(
        metric: Metric, entity: str, base_semantic_model: SemanticModel, conversion_semantic_model: SemanticModel
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        if entity not in {entity.name for entity in base_semantic_model.entities}:
            issues.append(
                ValidationError(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric.metadata),
                        metric=MetricModelReference(metric_name=metric.name),
                    ),
                    message=f"Entity: {entity} not found in base semantic model: {base_semantic_model.name}.",
                )
            )
        if entity not in {entity.name for entity in conversion_semantic_model.entities}:
            issues.append(
                ValidationError(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric.metadata),
                        metric=MetricModelReference(metric_name=metric.name),
                    ),
                    message=f"Entity: {entity} not found in "
                    f"conversion semantic model: {conversion_semantic_model.name}.",
                )
            )
        return issues

    @staticmethod
    def _validate_agg_and_expr(
        agg_type: AggregationType,
        expr: Optional[str],
        input_name: str,
        input_object_type: Literal["Measure", "Metric"],
        main_metric: Metric,
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        if (
            agg_type != AggregationType.COUNT
            and agg_type != AggregationType.COUNT_DISTINCT
            and (agg_type != AggregationType.SUM or expr != "1")
        ):
            issues.append(
                ValidationError(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=main_metric.metadata),
                        metric=MetricModelReference(metric_name=main_metric.name),
                    ),
                    message=f"For conversion metrics, the input {input_object_type.lower()} must be "
                    f"COUNT/SUM(1)/COUNT_DISTINCT. {input_object_type} '{input_name}' is agg type: {agg_type}",
                )
            )
        return issues

    @staticmethod
    def _validate_no_filter_for_conversion_input(
        filter: Optional[WhereFilterIntersection],
        input_name: str,
        input_object_type: Literal["Measure", "Metric"],
        is_base_input: bool,
        main_metric: Metric,
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        if filter is not None and not is_base_input:
            issues.append(
                ValidationWarning(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=main_metric.metadata),
                        metric=MetricModelReference(metric_name=main_metric.name),
                    ),
                    message=f"{input_object_type} input '{input_name}' has a filter. "
                    "For conversion metrics, filtering on the conversion "
                    "input is not fully supported yet. ",
                )
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checks that the provided measures are valid for conversion metrics")
    def _validate_measures(
        metric: Metric, base_semantic_model: SemanticModel, conversion_semantic_model: SemanticModel
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        def _validate_measure(
            input_measure: MetricInputMeasure,
            semantic_model: SemanticModel,
            is_base_input: bool = True,
        ) -> None:
            measure = None
            for model_measure in semantic_model.measures:
                if model_measure.reference == input_measure.measure_reference:
                    measure = model_measure
                    break

            assert measure, f"Measure '{model_measure.name}' wasn't found in semantic model '{semantic_model.name}'"

            issues.extend(
                ConversionMetricRule._validate_agg_and_expr(
                    agg_type=measure.agg,
                    expr=measure.expr,
                    input_name=measure.name,
                    input_object_type="Measure",
                    main_metric=metric,
                )
            )
            issues.extend(
                ConversionMetricRule._validate_no_filter_for_conversion_input(
                    filter=input_measure.filter,
                    input_name=measure.name,
                    input_object_type="Measure",
                    is_base_input=is_base_input,
                    main_metric=metric,
                )
            )

        conversion_type_params = PydanticMetric.get_checked_conversion_type_params(metric=metric)
        if conversion_type_params.base_measure is not None:
            # TODO SL-4116, SL-4188: mimic this validation for base_metric
            _validate_measure(
                input_measure=conversion_type_params.base_measure,
                semantic_model=base_semantic_model,
                is_base_input=True,
            )
        if conversion_type_params.conversion_measure is not None:
            # TODO SL-4116, SL-4188: mimic this validation for conversion_metric
            _validate_measure(
                input_measure=conversion_type_params.conversion_measure,
                semantic_model=conversion_semantic_model,
                is_base_input=False,
            )
        return issues

    @staticmethod
    def _validate_metrics(
        metric: Metric,
        semantic_manifest: SemanticManifest,
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        def _validate_metric(
            input_metric: MetricInput,
            semantic_manifest: SemanticManifest,
            is_base_metric: bool = True,
        ) -> None:
            metric = MetricValidationRuleHelpers.get_metric_from_manifest(input_metric.name, semantic_manifest)

            assert metric, f"Metric '{input_metric.name}' was not found.'"
            agg_params = metric.type_params.metric_aggregation_params
            assert agg_params, f"Metric '{input_metric.name}' is missing aggregation parameters "
            "such as the type of aggregation."

            issues.extend(
                ConversionMetricRule._validate_agg_and_expr(
                    agg_type=agg_params.agg,
                    expr=metric.type_params.expr,
                    input_name=metric.name,
                    input_object_type="Metric",
                    main_metric=metric,
                )
            )

            issues.extend(
                ConversionMetricRule._validate_no_filter_for_conversion_input(
                    filter=input_metric.filter,
                    input_name=metric.name,
                    input_object_type="Metric",
                    is_base_input=is_base_metric,
                    main_metric=metric,
                )
            )

        conversion_type_params = PydanticMetric.get_checked_conversion_type_params(metric=metric)
        if conversion_type_params.base_metric is not None:
            _validate_metric(
                input_metric=conversion_type_params.base_metric,
                semantic_manifest=semantic_manifest,
                is_base_metric=True,
            )
        if conversion_type_params.conversion_metric is not None:
            _validate_metric(
                input_metric=conversion_type_params.conversion_metric,
                semantic_manifest=semantic_manifest,
                is_base_metric=False,
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checks that the provided constant properties are valid")
    def _validate_constant_properties(
        metric: Metric, base_semantic_model: SemanticModel, conversion_semantic_model: SemanticModel
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        def _elements_in_model(references: List[str], semantic_model: SemanticModel) -> None:
            linkable_elements = [entity.name for entity in semantic_model.entities] + [
                dimension.name for dimension in semantic_model.dimensions
            ]
            for reference in references:
                if reference not in linkable_elements:
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message=f"The provided constant property: {reference}, "
                            f"cannot be found in semantic model {semantic_model.name}",
                        )
                    )

        conversion_type_params = PydanticMetric.get_checked_conversion_type_params(metric=metric)
        constant_properties = conversion_type_params.constant_properties or []
        base_properties = []
        conversion_properties = []
        for constant_property in constant_properties:
            base_properties.append(constant_property.base_property)
            conversion_properties.append(constant_property.conversion_property)

        _elements_in_model(references=base_properties, semantic_model=base_semantic_model)
        _elements_in_model(references=conversion_properties, semantic_model=conversion_semantic_model)
        return issues

    @staticmethod
    def _get_semantic_model_from_measure(
        measure_reference: MeasureReference, semantic_manifest: SemanticManifest
    ) -> Optional[SemanticModel]:
        """Retrieve the semantic model from a given measure reference."""
        semantic_model = None
        for model in semantic_manifest.semantic_models:
            if measure_reference in {measure.reference for measure in model.measures}:
                semantic_model = model
                break
        return semantic_model

    @staticmethod
    def _get_semantic_model_pointed_to_by_metric(
        metric_name: str, semantic_manifest: SemanticManifest
    ) -> Optional[SemanticModel]:
        """Retrieve the semantic model from a given metric reference.

        This is used to handle several steps of indirection - we get a MetricInput,
        which provides a metric name through which we can access the Metric, which
        then may point at a specific SemanticModel if it was defined as part of that
        model in its YAML specification.

        This returns None if any part of this look up chain fails.
        """
        semantic_model = None
        for model in semantic_manifest.semantic_models:
            metric = MetricValidationRuleHelpers.get_metric_from_manifest(metric_name, semantic_manifest)
            if (
                metric is not None
                and metric.type_params.metric_aggregation_params is not None
                and metric.type_params.metric_aggregation_params.semantic_model == model.name
            ):
                semantic_model = model
                break
        return semantic_model

    @staticmethod
    def _get_validated_model_for_input(
        input_measure: Optional[MetricInputMeasure],
        input_metric: Optional[MetricInput],
        metric_name: str,
        metric_metadata: Union[Metadata, None],
        input_type: Literal["base", "conversion"],
        semantic_manifest: SemanticManifest,
    ) -> Tuple[Optional[SemanticModel], List[ValidationIssue]]:
        issues: List[ValidationIssue] = []

        if input_metric is not None:
            real_input_metric = MetricValidationRuleHelpers.get_metric_from_manifest(
                input_metric.name,
                semantic_manifest,
            )
            if real_input_metric is not None and real_input_metric.type != MetricType.SIMPLE:
                issues.append(
                    ValidationError(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=real_input_metric.metadata),
                            metric=MetricModelReference(metric_name=real_input_metric.name),
                        ),
                        message=f"Metric '{real_input_metric.name}' is not a Simple metric, so it cannot "
                        f"be used as an input for Conversion metric '{metric_name}'.",
                    )
                )

        model: Optional[SemanticModel] = None
        if input_measure is not None and input_metric is not None:
            issues.append(
                ValidationWarning(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric_metadata),
                        metric=MetricModelReference(metric_name=metric_name),
                    ),
                    message=f"Conversion metric '{metric_name}' should not have both a {input_type} measure "
                    f"and a {input_type} metric as inputs. The measure input will be ignored; please "
                    "remove it to avoid confusion.",
                )
            )
        elif input_measure is None and input_metric is None:
            issues.append(
                ValidationError(
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric_metadata),
                        metric=MetricModelReference(metric_name=metric_name),
                    ),
                    message=f"Conversion metric '{metric_name}' must have either a {input_type} measure "
                    f"or a {input_type} metric as an input. Please add one of them.",
                )
            )
        elif input_measure is not None:
            model = ConversionMetricRule._get_semantic_model_from_measure(
                measure_reference=input_measure.measure_reference,
                semantic_manifest=semantic_manifest,
            )
            if model is None:
                input_measure_name = input_measure.measure_reference.element_name
                issues.append(
                    ValidationError(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=metric_metadata),
                            metric=MetricModelReference(metric_name=metric_name),
                        ),
                        message=f"Input measure '{input_measure_name}' for conversion metric "
                        f"'{metric_name}' does not exist in your manifest.",
                    )
                )

        elif input_metric is not None:
            # TODO - am i validating that the metric exists?
            input_metric_name = input_metric.name
            model = ConversionMetricRule._get_semantic_model_pointed_to_by_metric(
                metric_name=input_metric_name,
                semantic_manifest=semantic_manifest,
            )
            if model is None:
                issues.append(
                    ValidationError(
                        context=MetricContext(
                            file_context=FileContext.from_metadata(metadata=metric_metadata),
                            metric=MetricModelReference(metric_name=metric_name),
                        ),
                        message=f"Input metric '{input_metric_name}' for conversion metric "
                        f"'{metric_name}' is linked to a semantic model that does "
                        "not exist in your manifest.",
                    )
                )
        else:
            # since this depends on two inputs, we can't really use assert_never,
            # but we want to future proof this against mistakes in later maintenance.
            assert (
                False
            ), f"Failed to parse {input_type} model for conversion metric '{metric_name}' for unknown reason."
        return model, issues

    @staticmethod
    @validate_safely(whats_being_done="running manifest validation ensuring conversion metrics are valid")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []

        custom_granularity_names = {
            granularity.name
            for time_spine in semantic_manifest.project_configuration.time_spines
            for granularity in time_spine.custom_granularities
        }

        for metric in semantic_manifest.metrics or []:
            if metric.type == MetricType.CONVERSION:
                issues.extend(ConversionMetricRule._validate_measure_xor_metric_for_each_input(metric=metric))
                # Validates that the measure exists and corresponds to a semantic model
                conversion_type_params = PydanticMetric.get_checked_conversion_type_params(metric=metric)
                base_semantic_model, added_issues_from_base_model = ConversionMetricRule._get_validated_model_for_input(
                    input_measure=conversion_type_params.base_measure,
                    input_metric=conversion_type_params.base_metric,
                    metric_name=metric.name,
                    metric_metadata=metric.metadata,
                    input_type="base",
                    semantic_manifest=semantic_manifest,
                )
                issues.extend(added_issues_from_base_model)
                (
                    conversion_semantic_model,
                    added_issues_from_conversion_model,
                ) = ConversionMetricRule._get_validated_model_for_input(
                    input_measure=conversion_type_params.conversion_measure,
                    input_metric=conversion_type_params.conversion_metric,
                    metric_name=metric.name,
                    metric_metadata=metric.metadata,
                    input_type="conversion",
                    semantic_manifest=semantic_manifest,
                )
                issues.extend(added_issues_from_conversion_model)

                if base_semantic_model is None or conversion_semantic_model is None:
                    # If measure's don't exist, stop this metric's validation as it will fail later validations
                    continue

                issues += ConversionMetricRule._validate_entity_exists(
                    metric=metric,
                    entity=conversion_type_params.entity,
                    base_semantic_model=base_semantic_model,
                    conversion_semantic_model=conversion_semantic_model,
                )
                issues += ConversionMetricRule._validate_measures(
                    metric=metric,
                    base_semantic_model=base_semantic_model,
                    conversion_semantic_model=conversion_semantic_model,
                )
                issues += ConversionMetricRule._validate_metrics(
                    metric=metric,
                    semantic_manifest=semantic_manifest,
                )
                issues += ConversionMetricRule._validate_type_params(
                    metric=metric,
                    conversion_type_params=conversion_type_params,
                    custom_granularity_names=custom_granularity_names,
                )
                issues += ConversionMetricRule._validate_constant_properties(
                    metric=metric,
                    base_semantic_model=base_semantic_model,
                    conversion_semantic_model=conversion_semantic_model,
                )
        return issues


class MetricsNonAdditiveDimensionsRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that the non_additive_dimension for a metric is valid."""

    @staticmethod
    @validate_safely(whats_being_done="ensuring that a metric's non_additive_dimension is valid")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []
        semantic_models = semantic_manifest.semantic_models or []
        for metric in semantic_manifest.metrics or []:
            if (
                metric.type == MetricType.SIMPLE
                and metric.type_params is not None
                and metric.type_params.metric_aggregation_params is not None
                and metric.type_params.metric_aggregation_params.non_additive_dimension is not None
            ):
                model_iter = iter(
                    [
                        model
                        for model in semantic_models
                        if model.name == metric.type_params.metric_aggregation_params.semantic_model
                    ]
                )
                semantic_model = next(
                    model_iter,
                    None,
                )
                if not semantic_model:
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message=f"Metric '{metric.name}' references semantic model "
                            f"'{metric.type_params.metric_aggregation_params.semantic_model}', "
                            "but that semantic model could not be found.",
                        )
                    )
                    continue
                agg_time_dimension_reference = semantic_model.checked_agg_time_dimension_for_simple_metric(
                    metric=metric
                )
                issues.extend(
                    SharedMeasureAndMetricHelpers.validate_non_additive_dimension(
                        object=metric,
                        semantic_model=semantic_model,
                        non_additive_dimension=metric.type_params.metric_aggregation_params.non_additive_dimension,
                        agg_time_dimension_reference=agg_time_dimension_reference,
                        object_type_for_errors="Metric",
                    )
                )
        return issues


class MetricsCountAggregationExprRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that COUNT metrics have an expr provided."""

    @staticmethod
    @validate_safely(whats_being_done="validating the expr for metrics with COUNT aggregation")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []

        for metric in semantic_manifest.metrics or []:
            if (
                metric.type == MetricType.SIMPLE
                and metric.type_params is not None
                and metric.type_params.metric_aggregation_params is not None
                and metric.type_params.metric_aggregation_params.agg == AggregationType.COUNT
            ):
                context = MetricContext(
                    file_context=FileContext.from_metadata(metadata=metric.metadata),
                    metric=MetricModelReference(metric_name=metric.name),
                )
                issues.extend(
                    SharedMeasureAndMetricHelpers.validate_expr_for_count_aggregation(
                        context=context,
                        object_name=metric.name,
                        object_type="Metric",
                        agg_type=metric.type_params.metric_aggregation_params.agg,
                        expr=metric.type_params.expr,
                    )
                )
        return issues


class MetricsPercentileAggregationRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that only PERCENTILE metrics have agg_params and a valid percentile value is provided."""

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring the agg_params.percentile value exist for metrics with "
        "percentile aggregation"
    )
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []

        for metric in semantic_manifest.metrics or []:
            if (
                metric.type == MetricType.SIMPLE
                and metric.type_params is not None
                and metric.type_params.metric_aggregation_params is not None
            ):
                context = MetricContext(
                    file_context=FileContext.from_metadata(metadata=metric.metadata),
                    metric=MetricModelReference(metric_name=metric.name),
                )
                issues.extend(
                    SharedMeasureAndMetricHelpers.validate_percentile_arguments(
                        context=context,
                        object_name=metric.name,
                        object_type="Metric",
                        agg_type=metric.type_params.metric_aggregation_params.agg,
                        agg_params=metric.type_params.metric_aggregation_params.agg_params,
                    )
                )

        return issues


class MetricAggregationParamsInForSimpleMetricsRule(
    SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]
):
    """Checks that metric aggregation params are only set for simple metrics."""

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring metric aggregation params are only set for simple metrics"
    )
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D102
        issues: List[ValidationIssue] = []

        for metric in semantic_manifest.metrics or []:
            # Non-simple metrics cannot use these measure-like fields!
            if metric.type != MetricType.SIMPLE:
                if metric.type_params.metric_aggregation_params is not None:
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message=f"Metric '{metric.name}' is not a Simple metric, so it cannot have values for "
                            "'agg', 'agg_time_dimension', 'non_additive_dimension', 'percentile', or 'expr'.",
                        )
                    )
                other_illegal_fields = []
                if metric.type_params.fill_nulls_with is not None:
                    other_illegal_fields.append("fill_nulls_with")
                if metric.type_params.join_to_timespine:
                    other_illegal_fields.append("join_to_timespine")
                if other_illegal_fields:
                    other_illegal_fields.sort()
                    # Ruff struggles with nested f-strings, so we must do this in two steps.
                    other_illegal_fields = [f"'{f}'" for f in other_illegal_fields]
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message=f"Metric '{metric.name}' is not a Simple metric, so it cannot have a value for "
                            f"for the following fields: {', '.join(other_illegal_fields)}.",
                        )
                    )
            # Simple metrics must have agg_params (measure-like fields) XOR an input measure
            if metric.type == MetricType.SIMPLE:
                has_agg_params = metric.type_params.metric_aggregation_params is not None
                has_input_measure = metric.type_params.measure is not None
                if has_input_measure:
                    if has_agg_params:
                        issues.append(
                            ValidationWarning(
                                context=MetricContext(
                                    file_context=FileContext.from_metadata(metadata=metric.metadata),
                                    metric=MetricModelReference(metric_name=metric.name),
                                ),
                                message=f"Metric '{metric.name}' should not have both "
                                "metric_aggregation_params and a measure. The measure will be ignored; "
                                "please remove it to avoid confusion.",
                            )
                        )
                    if metric.type_params.fill_nulls_with is not None:
                        issues.append(
                            ValidationWarning(
                                context=MetricContext(
                                    file_context=FileContext.from_metadata(metadata=metric.metadata),
                                    metric=MetricModelReference(metric_name=metric.name),
                                ),
                                message=f"Simple Metric '{metric.name}' should not have a measure input as well as a "
                                "value for fill_nulls_with.  The metric's fill_nulls_with "
                                "will be ignored until the measure is removed.",
                            )
                        )
                    if metric.type_params.join_to_timespine:
                        issues.append(
                            ValidationWarning(
                                context=MetricContext(
                                    file_context=FileContext.from_metadata(metadata=metric.metadata),
                                    metric=MetricModelReference(metric_name=metric.name),
                                ),
                                message=f"Simple Metric '{metric.name}' should not have a measure input as well as a "
                                "value for join_to_timespine.  The metric's join_to_timespine "
                                "will be ignored until the measure is removed.",
                            )
                        )
                elif not has_agg_params and not has_input_measure:
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                file_context=FileContext.from_metadata(metadata=metric.metadata),
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message=f"Metric '{metric.name}' is a Simple metric, so it must have either "
                            "metric_aggregation_params or a measure.",
                        )
                    )

        return issues


class MetricTimeGranularityRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Checks that time_granularity set for metric is queryable for that metric."""

    @staticmethod
    def _min_queryable_granularity_for_metric(
        metric: Metric,
        metric_index: Dict[MetricReference, Metric],
        measure_to_agg_time_dimension: Dict[MeasureReference, Optional[Dimension]],
    ) -> Optional[TimeGranularity]:
        """Get the minimum time granularity this metric is allowed to be queried with.

        This should be the largest granularity that any of the metric's agg_time_dimensions is defined at.
        Defaults to DAY in the
        """
        min_queryable_granularity: Optional[TimeGranularity] = None
        for measure_reference in PydanticMetric.all_input_measures_for_metric(metric=metric, metric_index=metric_index):
            agg_time_dimension = measure_to_agg_time_dimension.get(measure_reference)
            if not agg_time_dimension:
                # This indicates the measure or agg_time_dimension were invalid, so we can't determine granularity.
                return None
            defined_time_granularity = (
                agg_time_dimension.type_params.time_granularity
                if agg_time_dimension.type_params
                else TimeGranularity.DAY
            )
            if not min_queryable_granularity or defined_time_granularity.to_int() > min_queryable_granularity.to_int():
                min_queryable_granularity = defined_time_granularity

        return min_queryable_granularity

    @staticmethod
    @validate_safely(
        whats_being_done="running model validation ensuring a metric's time_granularity is valid for the metric"
    )
    def _validate_metric(
        metric: Metric,
        metric_index: Dict[MetricReference, Metric],
        measure_to_agg_time_dimension: Dict[MeasureReference, Optional[Dimension]],
    ) -> Sequence[ValidationIssue]:  # noqa: D103
        issues: List[ValidationIssue] = []
        context = MetricContext(
            file_context=FileContext.from_metadata(metadata=metric.metadata),
            metric=MetricModelReference(metric_name=metric.name),
        )

        if metric.time_granularity:
            min_queryable_granularity = MetricTimeGranularityRule._min_queryable_granularity_for_metric(
                metric=metric, metric_index=metric_index, measure_to_agg_time_dimension=measure_to_agg_time_dimension
            )
            if not min_queryable_granularity:
                issues.append(
                    ValidationError(
                        context=context,
                        message=(
                            f"Unable to validate `time_granularity` for metric '{metric.name}' due to "
                            "misconfiguration with measures or related agg_time_dimensions."
                        ),
                    )
                )
                return issues
            valid_granularities = [
                granularity.value
                for granularity in TimeGranularity
                if granularity.to_int() >= min_queryable_granularity.to_int()
            ]
            if metric.time_granularity not in valid_granularities:
                issues.append(
                    ValidationError(
                        context=context,
                        message=(
                            f"`time_granularity` for metric '{metric.name}' must be >= "
                            f"{min_queryable_granularity.name}. Valid options are those that are >= the largest "
                            f"granularity defined for the metric's measures' agg_time_dimensions. Got: "
                            f"{metric.time_granularity}. Valid options: {valid_granularities}"
                        ),
                    )
                )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="running manifest validation ensuring metric time_granularitys are valid")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:
        """Validate that the time_granularity for each metric is queryable for that metric.

        TODO: figure out a more efficient way to reference other aspects of the model. This validation essentially
        requires parsing the entire model, which could be slow and likely is repeated work. The blocker is that the
        inputs to validations are protocols, which don't easily store parsed metadata.
        """
        issues: List[ValidationIssue] = []

        measure_to_agg_time_dimension: Dict[MeasureReference, Optional[Dimension]] = {}
        for semantic_model in semantic_manifest.semantic_models:
            dimension_index = {DimensionReference(dimension.name): dimension for dimension in semantic_model.dimensions}
            for measure in semantic_model.measures:
                try:
                    agg_time_dimension_ref = semantic_model.checked_agg_time_dimension_for_measure(measure.reference)
                    agg_time_dimension: Optional[Dimension] = dimension_index[
                        agg_time_dimension_ref.dimension_reference
                    ]
                except (AssertionError, KeyError):
                    # If the agg_time_dimension is not set or does not exist, this will be validated elsewhere.
                    # Here, swallow the error to avoid disrupting the validation process.
                    agg_time_dimension = None
                measure_to_agg_time_dimension[measure.reference] = agg_time_dimension

        metric_index = {MetricReference(metric.name): metric for metric in semantic_manifest.metrics}
        for metric in semantic_manifest.metrics or []:
            issues += MetricTimeGranularityRule._validate_metric(
                metric=metric,
                metric_index=metric_index,
                measure_to_agg_time_dimension=measure_to_agg_time_dimension,
            )
        return issues
