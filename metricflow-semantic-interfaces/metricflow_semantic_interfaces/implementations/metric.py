from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Sequence, Set

from msi_pydantic_shim import Field
from typing_extensions import override

from metricflow_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantic_interfaces.errors import ParsingException
from metricflow_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
    PydanticCustomInputParser,
    PydanticParseableValueType,
)
from metricflow_semantic_interfaces.implementations.element_config import (
    PydanticSemanticLayerElementConfig,
)
from metricflow_semantic_interfaces.implementations.elements.measure import (
    PydanticMeasure,
    PydanticMeasureAggregationParameters,
    PydanticNonAdditiveDimensionParameters,
)
from metricflow_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilterIntersection,
)
from metricflow_semantic_interfaces.implementations.metadata import PydanticMetadata
from metricflow_semantic_interfaces.protocols import Metric, ProtocolHint
from metricflow_semantic_interfaces.protocols.metric import ConversionTypeParams
from metricflow_semantic_interfaces.references import MeasureReference, MetricReference
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    ConversionCalculationType,
    MetricType,
    PeriodAggregation,
    TimeGranularity,
)


class PydanticMetricInputMeasure(PydanticCustomInputParser, HashableBaseModel):
    """Provides a pointer to a measure along with metric-specific processing directives.

    If an alias is set, this will be used as the string name reference for this measure after the aggregation
    phase in the SQL plan.
    """

    name: str
    filter: Optional[PydanticWhereFilterIntersection]
    alias: Optional[str]
    join_to_timespine: bool = False
    fill_nulls_with: Optional[int] = None

    @classmethod
    def _from_yaml_value(cls, input: PydanticParseableValueType) -> PydanticMetricInputMeasure:
        """Parses a MetricInputMeasure from a string (name only) or object (struct spec) input.

        For user input cases, the original YAML spec for a PydanticMetric included measure(s) specified as string names
        or lists of string names. As such, configs pre-dating the addition of this model type will only provide the
        base name for this object.
        """
        if isinstance(input, str):
            return PydanticMetricInputMeasure(name=input)
        else:
            raise ValueError(
                f"MetricInputMeasure inputs from model configs are expected to be of either type string or "
                f"object (key/value pairs), but got type {type(input)} with value: {input}"
            )

    @property
    def measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference associated with this metric input measure."""
        return MeasureReference(element_name=self.name)

    @property
    def post_aggregation_measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference with the aliased name, if appropriate."""
        return MeasureReference(element_name=self.alias or self.name)


class PydanticMetricTimeWindow(PydanticCustomInputParser, HashableBaseModel):
    """Describes the window of time the metric should be accumulated over, e.g., '1 day', '2 weeks', etc."""

    count: int
    granularity: str

    @classmethod
    def _from_yaml_value(cls, input: PydanticParseableValueType) -> PydanticMetricTimeWindow:
        """Parses a MetricTimeWindow from a string input found in a user provided model specification.

        The MetricTimeWindow is always expected to be provided as a string in user-defined YAML configs.
        """
        if isinstance(input, str):
            return PydanticMetricTimeWindow.parse(window=input.lower())
        else:
            raise ValueError(
                f"MetricTimeWindow inputs from model configs are expected to always be of type string, but got "
                f"type {type(input)} with value: {input}"
            )

    @property
    def is_standard_granularity(self) -> bool:
        """Returns whether the window uses standard TimeGranularity."""
        return self.granularity.casefold() in {item.value.casefold() for item in TimeGranularity}

    @property
    def window_string(self) -> str:
        """Returns the string value of the time window."""
        return f"{self.count} {self.granularity}"

    @staticmethod
    def parse(window: str) -> PydanticMetricTimeWindow:
        """Returns window values if parsing succeeds, None otherwise."""
        parts = window.lower().split(" ")
        if len(parts) != 2:
            raise ParsingException(
                f"Invalid window ({window}) in cumulative metric. Should be of the form `<count> <granularity>`, "
                "e.g., `28 days`",
            )

        granularity = parts[1]
        count = parts[0]
        if not count.isdigit():
            raise ParsingException(f"Invalid count ({count}) in cumulative metric window string: ({window})")

        return PydanticMetricTimeWindow(
            count=int(count),
            granularity=granularity,
        )


class PydanticConstantPropertyInput(HashableBaseModel):
    """Input of a constant property used in conversion metrics."""

    base_property: str
    conversion_property: str


class PydanticMetricInput(HashableBaseModel):
    """Provides a pointer to a metric along with the additional properties used on that metric."""

    name: str
    filter: Optional[PydanticWhereFilterIntersection]
    alias: Optional[str]
    offset_window: Optional[PydanticMetricTimeWindow]
    offset_to_grain: Optional[str]

    @property
    def as_reference(self) -> MetricReference:
        """Property accessor to get the MetricReference associated with this metric input."""
        return MetricReference(element_name=self.name)

    @property
    def post_aggregation_reference(self) -> MetricReference:
        """Property accessor to get the MetricReference with the aliased name, if appropriate."""
        return MetricReference(element_name=self.alias or self.name)


class PydanticConversionTypeParams(HashableBaseModel):
    """Type params to provide context for conversion metrics properties."""

    base_measure: Optional[PydanticMetricInputMeasure]
    base_metric: Optional[PydanticMetricInput]
    conversion_measure: Optional[PydanticMetricInputMeasure]
    conversion_metric: Optional[PydanticMetricInput]
    entity: str
    calculation: ConversionCalculationType = ConversionCalculationType.CONVERSION_RATE
    window: Optional[PydanticMetricTimeWindow]
    constant_properties: Optional[List[PydanticConstantPropertyInput]]


class PydanticCumulativeTypeParams(HashableBaseModel):
    """Type params to provide context for cumulative metrics properties."""

    window: Optional[PydanticMetricTimeWindow]
    grain_to_date: Optional[str]
    period_agg: PeriodAggregation = PeriodAggregation.FIRST
    metric: Optional[PydanticMetricInput]


class PydanticMetricAggregationParams(HashableBaseModel):
    """Type params to provide context for metrics that are used as source nodes."""

    semantic_model: str

    # If you add fields to this, please make sure to update the transformation
    # helper PydanticMeasure.to_metric_aggregation_params()
    agg: AggregationType
    agg_params: Optional[PydanticMeasureAggregationParameters]
    agg_time_dimension: Optional[str]
    non_additive_dimension: Optional[PydanticNonAdditiveDimensionParameters]


class PydanticMetricTypeParams(HashableBaseModel):
    """Type params add additional context to certain metric types (the context depends on the metric type)."""

    measure: Optional[PydanticMetricInputMeasure]
    numerator: Optional[PydanticMetricInput]
    denominator: Optional[PydanticMetricInput]
    expr: Optional[str]
    # Legacy, supports custom grain through PydanticMetricTimeWindow changes (should deprecate though)
    window: Optional[PydanticMetricTimeWindow]
    # Legacy, will not support custom granularity
    grain_to_date: Optional[TimeGranularity]
    # Only used for derived metrics so far
    metrics: Optional[List[PydanticMetricInput]]
    conversion_type_params: Optional[PydanticConversionTypeParams]
    cumulative_type_params: Optional[PydanticCumulativeTypeParams]

    input_measures: List[PydanticMetricInputMeasure] = Field(default_factory=list)

    # TODO SL-4116: Validate that we accept measure-only config fields here IFF
    # this is a simple metric and does not have a measure argument.
    # This field is required and allowed IFF this metric is a simple metric
    # that does not have any measure arguments.
    metric_aggregation_params: Optional[PydanticMetricAggregationParams]

    # These fields are allowed for simple metrics only.
    # Previously, these lived in the "PydanticMetricInput",
    # which was only everattached to a consumer metric.  Now, they are attached to the
    # producing metric, which may require more total metrics to be created.
    # TODO: SL-4116: Add validation that these are only on simple metrics.
    join_to_timespine: bool = False
    fill_nulls_with: Optional[int] = None

    # Indicates the metric exposed to the users and APIs.
    # Generally used for metrics we create implicitly to replace measures, but
    # eventually we'll also enable users to set this value on metrics in their YAML as well.
    is_private: Optional[bool] = False


class PydanticMetric(HashableBaseModel, ModelWithMetadataParsing, ProtocolHint[Metric]):
    """Describes a metric."""

    @override
    def _implements_protocol(self) -> Metric:  # noqa: D102
        return self

    name: str
    description: Optional[str]
    type: MetricType
    type_params: PydanticMetricTypeParams
    filter: Optional[PydanticWhereFilterIntersection]
    metadata: Optional[PydanticMetadata]
    label: Optional[str] = None
    config: Optional[PydanticSemanticLayerElementConfig]
    time_granularity: Optional[str] = None

    @classmethod
    def parse_obj(cls, input: Any) -> PydanticMetric:  # type: ignore[misc]
        """Adds custom parsing to the default method."""
        data = deepcopy(input)

        # Ensure grain_to_date is lowercased
        type_params = data.get("type_params") or {}
        grain_to_date = (type_params.get("cumulative_type_params") or {}).get("grain_to_date")
        if isinstance(grain_to_date, str):
            data["type_params"]["cumulative_type_params"]["grain_to_date"] = grain_to_date.lower()

        # Ensure offset_to_grain is lowercased (only used in derived metrics)
        input_metrics = type_params.get("metrics", [])
        if input_metrics:
            for input_metric in input_metrics:
                offset_to_grain = input_metric.get("offset_to_grain")
                if offset_to_grain and isinstance(offset_to_grain, str):
                    input_metric["offset_to_grain"] = offset_to_grain.lower()

        return super(HashableBaseModel, cls).parse_obj(data)

    @property
    def input_measures(self) -> Sequence[PydanticMetricInputMeasure]:
        """Return the complete list of input measure configurations for this metric."""
        return self.type_params.input_measures

    @property
    def measure_references(self) -> List[MeasureReference]:
        """Return the measure references associated with all input measure configurations for this metric."""
        return [x.measure_reference for x in self.input_measures]

    @property
    def input_metrics(self) -> Sequence[PydanticMetricInput]:
        """Return the associated input metrics for this metric."""
        if self.type is MetricType.SIMPLE or self.type is MetricType.CUMULATIVE or self.type is MetricType.CONVERSION:
            return ()
        elif self.type is MetricType.DERIVED:
            assert self.type_params.metrics is not None, f"{MetricType.DERIVED} should have type_params.metrics set"
            return self.type_params.metrics
        elif self.type is MetricType.RATIO:
            assert (
                self.type_params.numerator is not None and self.type_params.denominator is not None
            ), f"{self} is metric type {MetricType.RATIO}, so neither the numerator and denominator should not be None"
            return (self.type_params.numerator, self.type_params.denominator)
        elif self.type is MetricType.CONVERSION:
            conversion_type_params = PydanticMetric.get_checked_conversion_type_params(metric=self)
            metrics: Set[PydanticMetricInput] = set()
            if conversion_type_params.base_metric is not None:
                metrics.add(conversion_type_params.base_metric)
            if conversion_type_params.conversion_metric is not None:
                metrics.add(conversion_type_params.conversion_metric)
            return list(metrics)
        else:
            assert_values_exhausted(self.type)

    @staticmethod
    def all_input_measures_for_metric(
        metric: Metric, metric_index: Dict[MetricReference, Metric]
    ) -> Set[MeasureReference]:
        """Gets all input measures for the metric, including those defined on input metrics (recursively)."""
        measures: Set[MeasureReference] = set()
        if metric.type is MetricType.SIMPLE or metric.type is MetricType.CUMULATIVE:
            assert (
                metric.type_params.measure is not None
            ), f"Metric {metric.name} should have a measure defined, but it does not."
            measures.add(metric.type_params.measure.measure_reference)
        elif metric.type is MetricType.DERIVED or metric.type is MetricType.RATIO:
            for input_metric in metric.input_metrics:
                nested_metric = metric_index.get(input_metric.as_reference)
                assert nested_metric, f"Could not find metric {input_metric.name} in semantic manifest."
                measures.update(
                    PydanticMetric.all_input_measures_for_metric(metric=nested_metric, metric_index=metric_index)
                )
        elif metric.type is MetricType.CONVERSION:
            conversion_type_params = PydanticMetric.get_checked_conversion_type_params(metric=metric)
            if conversion_type_params.base_measure is not None:
                measures.add(conversion_type_params.base_measure.measure_reference)
            if conversion_type_params.conversion_measure is not None:
                measures.add(conversion_type_params.conversion_measure.measure_reference)
        else:
            assert_values_exhausted(metric.type)

        return measures

    @staticmethod
    def get_checked_conversion_type_params(metric: Metric) -> ConversionTypeParams:
        """Returns the conversion type params for a metric, checking that they are valid."""
        assert metric.type is MetricType.CONVERSION, "Only conversion metrics can have conversion type params."
        conversion_type_params = metric.type_params.conversion_type_params
        assert conversion_type_params, f"Conversion metric '{metric.name}' must have conversion_type_params."
        return conversion_type_params

    @staticmethod
    def build_metric_aggregation_params(
        measure: PydanticMeasure,
        semantic_model_name: str,
    ) -> PydanticMetricAggregationParams:
        """This helps us create simple metrics from measures.

        It lives here instead of measures to avoid circular import issues.
        """
        agg_params = measure.agg_params.copy(deep=True) if measure.agg_params is not None else None
        non_additive_dimension = (
            measure.non_additive_dimension.copy(deep=True) if measure.non_additive_dimension is not None else None
        )
        return PydanticMetricAggregationParams(
            semantic_model=semantic_model_name,
            agg=measure.agg,
            agg_params=agg_params,
            agg_time_dimension=measure.agg_time_dimension,
            non_additive_dimension=non_additive_dimension,
        )
