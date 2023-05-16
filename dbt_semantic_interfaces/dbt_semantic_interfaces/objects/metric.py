from __future__ import annotations

from typing import List, Optional

from dbt_semantic_interfaces.enum_extension import ExtendedEnum
from dbt_semantic_interfaces.errors import ParsingException
from dbt_semantic_interfaces.objects.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
    PydanticCustomInputParser,
    PydanticParseableValueType,
)
from dbt_semantic_interfaces.objects.filters.where_filter import WhereFilter
from dbt_semantic_interfaces.objects.metadata import Metadata
from dbt_semantic_interfaces.objects.time_granularity import (
    TimeGranularity,
    string_to_time_granularity,
)
from dbt_semantic_interfaces.references import MeasureReference, MetricReference


class MetricType(ExtendedEnum):
    """Currently supported metric types."""

    MEASURE_PROXY = "measure_proxy"
    RATIO = "ratio"
    EXPR = "expr"
    CUMULATIVE = "cumulative"
    DERIVED = "derived"


class MetricInputMeasure(PydanticCustomInputParser, HashableBaseModel):
    """Provides a pointer to a measure along with metric-specific processing directives.

    If an alias is set, this will be used as the string name reference for this measure after the aggregation
    phase in the SQL plan.
    """

    name: str
    filter: Optional[WhereFilter]
    alias: Optional[str]

    @classmethod
    def _from_yaml_value(cls, input: PydanticParseableValueType) -> MetricInputMeasure:
        """Parses a MetricInputMeasure from a string (name only) or object (struct spec) input.

        For user input cases, the original YAML spec for a Metric included measure(s) specified as string names
        or lists of string names. As such, configs pre-dating the addition of this model type will only provide the
        base name for this object.
        """
        if isinstance(input, str):
            return MetricInputMeasure(name=input)
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


class MetricTimeWindow(PydanticCustomInputParser, HashableBaseModel):
    """Describes the window of time the metric should be accumulated over, e.g., '1 day', '2 weeks', etc."""

    count: int
    granularity: TimeGranularity

    def to_string(self) -> str:  # noqa: D
        return f"{self.count} {self.granularity.value}"

    @classmethod
    def _from_yaml_value(cls, input: PydanticParseableValueType) -> MetricTimeWindow:
        """Parses a MetricTimeWindow from a string input found in a user provided model specification.

        The MetricTimeWindow is always expected to be provided as a string in user-defined YAML configs.
        """
        if isinstance(input, str):
            return MetricTimeWindow.parse(input)
        else:
            raise ValueError(
                f"MetricTimeWindow inputs from model configs are expected to always be of type string, but got "
                f"type {type(input)} with value: {input}"
            )

    @staticmethod
    def parse(window: str) -> MetricTimeWindow:
        """Returns window values if parsing succeeds, None otherwise.

        Output of the form: (<time unit count>, <time granularity>, <error message>) - error message is None if window
        is formatted properly
        """
        parts = window.split(" ")
        if len(parts) != 2:
            raise ParsingException(
                f"Invalid window ({window}) in cumulative metric. Should be of the form `<count> <granularity>`, "
                "e.g., `28 days`",
            )

        granularity = parts[1]
        # if we switched to python 3.9 this could just be `granularity = parts[0].removesuffix('s')
        if granularity.endswith("s"):
            # months -> month
            granularity = granularity[:-1]
        if granularity not in [item.value for item in TimeGranularity]:
            raise ParsingException(
                f"Invalid time granularity {granularity} in cumulative metric window string: ({window})",
            )

        count = parts[0]
        if not count.isdigit():
            raise ParsingException(f"Invalid count ({count}) in cumulative metric window string: ({window})")

        return MetricTimeWindow(
            count=int(count),
            granularity=string_to_time_granularity(granularity),
        )


class MetricInput(HashableBaseModel):
    """Provides a pointer to a metric along with the additional properties used on that metric."""

    name: str
    filter: Optional[WhereFilter]
    alias: Optional[str]
    offset_window: Optional[MetricTimeWindow]
    offset_to_grain: Optional[TimeGranularity]

    @property
    def as_reference(self) -> MetricReference:  # noqa: D
        return MetricReference(element_name=self.name)


class MetricTypeParams(HashableBaseModel):
    """Type params add additional context to certain metric types (the context depends on the metric type)."""

    measure: Optional[MetricInputMeasure]
    measures: Optional[List[MetricInputMeasure]]
    numerator: Optional[MetricInputMeasure]
    denominator: Optional[MetricInputMeasure]
    expr: Optional[str]
    window: Optional[MetricTimeWindow]
    grain_to_date: Optional[TimeGranularity]
    metrics: Optional[List[MetricInput]]

    @property
    def numerator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the numerator."""
        return self.numerator.measure_reference if self.numerator else None

    @property
    def denominator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the denominator."""
        return self.denominator.measure_reference if self.denominator else None


class Metric(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a metric."""

    name: str
    description: Optional[str]
    type: MetricType
    type_params: MetricTypeParams
    filter: Optional[WhereFilter]
    metadata: Optional[Metadata]

    @property
    def input_measures(self) -> List[MetricInputMeasure]:
        """Return the complete list of input measure configurations for this metric."""
        tp = self.type_params
        res = tp.measures or []
        if tp.measure:
            res.append(tp.measure)
        if tp.numerator:
            res.append(tp.numerator)
        if tp.denominator:
            res.append(tp.denominator)

        return res

    @property
    def measure_references(self) -> List[MeasureReference]:
        """Return the measure references associated with all input measure configurations for this metric."""
        return [x.measure_reference for x in self.input_measures]

    @property
    def input_metrics(self) -> List[MetricInput]:
        """Return the associated input metrics for this metric."""
        return self.type_params.metrics or []
