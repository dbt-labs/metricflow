from __future__ import annotations

from pydantic import validator
from typing import Any, List, Optional

from metricflow.errors.errors import ParsingException
from metricflow.model.objects.common import Metadata
from metricflow.model.objects.constraints.where import WhereClauseConstraint
from metricflow.model.objects.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
    PydanticCustomInputParser,
    PydanticParseableValueType,
)
from metricflow.object_utils import ExtendedEnum, hash_items
from metricflow.references import MeasureReference
from metricflow.time.time_granularity import TimeGranularity
from metricflow.time.time_granularity import string_to_time_granularity


class MetricType(ExtendedEnum):
    """Currently supported metric types"""

    MEASURE_PROXY = "measure_proxy"
    RATIO = "ratio"
    EXPR = "expr"
    CUMULATIVE = "cumulative"
    DERIVED = "derived"
    CONVERSION = "conversion"


class ConversionCalculationType(ExtendedEnum):
    """Types of calculations for a conversion metric."""

    CONVERSIONS = "conversions"
    CONVERSION_RATE = "conversion_rate"


class MetricInputMeasure(PydanticCustomInputParser, HashableBaseModel):
    """Provides a pointer to a measure along with metric-specific processing directives

    If an alias is set, this will be used as the string name reference for this measure after the aggregation
    phase in the SQL plan.
    """

    name: str
    constraint: Optional[WhereClauseConstraint]
    alias: Optional[str]

    @classmethod
    def _from_yaml_value(cls, input: PydanticParseableValueType) -> MetricInputMeasure:
        """Parses a MetricInputMeasure from a string (name only) or object (struct spec) input

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
        """Property accessor to get the MeasureReference associated with this metric input measure"""
        return MeasureReference(element_name=self.name)

    @property
    def post_aggregation_measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference with the aliased name, if appropriate"""
        return MeasureReference(element_name=self.alias or self.name)


class MetricTimeWindow(PydanticCustomInputParser, HashableBaseModel):
    """Describes the window of time the metric should be accumulated over, e.g., '1 day', '2 weeks', etc"""

    count: int
    granularity: TimeGranularity

    def to_string(self) -> str:  # noqa: D
        return f"{self.count} {self.granularity.value}"

    @classmethod
    def _from_yaml_value(cls, input: PydanticParseableValueType) -> MetricTimeWindow:
        """Parses a MetricTimeWindow from a string input found in a user provided model specification

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
        """Returns window values if parsing succeeds, None otherwise

        Output of the form: (<time unit count>, <time granularity>, <error message>) - error message is None if window is formatted properly
        """
        parts = window.split(" ")
        if len(parts) != 2:
            raise ParsingException(
                f"Invalid window ({window}) in cumulative metric. Should be of the form `<count> <granularity>`, e.g., `28 days`",
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
    constraint: Optional[WhereClauseConstraint]
    alias: Optional[str]
    offset_window: Optional[MetricTimeWindow]
    offset_to_grain: Optional[TimeGranularity]


class ConstantPropertyInput(HashableBaseModel):
    """Input of a constant property used in conversion metrics."""

    name: str
    base_expr: Optional[str]
    conversion_expr: Optional[str]

    @validator("conversion_expr", "base_expr", always=True)
    @classmethod
    def default_expr_value(cls, value: Any, values: Any) -> str:  # type: ignore[misc]
        """Defaulting the value of the constant property 'expr' value using pydantic validator

        If a expr value is provided that is a string, that will become the value of expr.
        If the provifed expr value is None, the expr value becomes the
        name of the constant property.
        """

        if value is None:
            if "name" not in values:
                raise ValueError("Failed to default expr value because objects name value was not defined")
            value = values["name"]

        # guarantee value is string
        if not isinstance(value, str):
            raise ValueError(f"expr value should be a string (str) type, but got {type(value)} with value: {value}")
        return value


class ConversionTypeParams(HashableBaseModel):
    """Type params to provide context for conversion metrics."""

    base_measure: MetricInputMeasure
    conversion_measure: MetricInputMeasure
    entity: str
    calculation: ConversionCalculationType = ConversionCalculationType.CONVERSION_RATE
    window: Optional[MetricTimeWindow]
    constant_properties: Optional[List[ConstantPropertyInput]]

    @property
    def base_measure_reference(self) -> MeasureReference:
        """Return the measure reference associated with the base measure."""
        return self.base_measure.measure_reference

    @property
    def conversion_measure_reference(self) -> MeasureReference:
        """Return the measure reference associated with the conversion measure."""
        return self.conversion_measure.measure_reference


class MetricTypeParams(HashableBaseModel):
    """Type params add additional context to certain metric types (the context depends on the metric type)"""

    measure: Optional[MetricInputMeasure]
    measures: Optional[List[MetricInputMeasure]]
    numerator: Optional[MetricInputMeasure]
    denominator: Optional[MetricInputMeasure]
    expr: Optional[str]
    window: Optional[MetricTimeWindow]
    grain_to_date: Optional[TimeGranularity]
    metrics: Optional[List[MetricInput]]
    conversion_type_params: Optional[ConversionTypeParams]

    @property
    def numerator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the numerator"""
        return self.numerator.measure_reference if self.numerator else None

    @property
    def denominator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the denominator"""
        return self.denominator.measure_reference if self.denominator else None

    @property
    def conversion_params(self) -> ConversionTypeParams:
        """Accessor for conversion type params, enforces that it's set."""
        if self.conversion_type_params is None:
            raise ValueError("conversion_type_params is not defined.")
        return self.conversion_type_params


class Metric(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a metric"""

    name: str
    description: Optional[str]
    type: MetricType
    type_params: MetricTypeParams
    constraint: Optional[WhereClauseConstraint]
    metadata: Optional[Metadata]

    @property
    def input_measures(self) -> List[MetricInputMeasure]:
        """Return the complete list of input measure configurations for this metric"""
        tp = self.type_params
        res = tp.measures or []
        if tp.measure:
            res.append(tp.measure)
        if tp.numerator:
            res.append(tp.numerator)
        if tp.denominator:
            res.append(tp.denominator)
        if tp.conversion_type_params:
            res.append(tp.conversion_type_params.base_measure)
            res.append(tp.conversion_type_params.conversion_measure)
        return res

    @property
    def measure_references(self) -> List[MeasureReference]:
        """Return the measure references associated with all input measure configurations for this metric"""
        return [x.measure_reference for x in self.input_measures]

    @property
    def input_metrics(self) -> List[MetricInput]:
        """Return the associated input metrics for this metric"""
        return self.type_params.metrics or []

    @property
    def definition_hash(self) -> str:  # noqa: D
        values: List[str] = [self.name, self.type_params.expr or ""]
        if self.constraint:
            values.append(self.constraint.where)
            if self.constraint.linkable_names:
                values.extend(self.constraint.linkable_names)
        values.extend([m.element_name for m in self.measure_references])
        return hash_items(values)

    @property
    def conversion_type_params(self) -> ConversionTypeParams:
        """Accessor for conversion type params, enforces that it's set."""
        assert self.type == MetricType.CONVERSION, "Should only access this for a conversion metric."
        if self.type_params.conversion_type_params is None:
            raise ValueError("conversion_type_params is not defined.")
        return self.type_params.conversion_type_params
