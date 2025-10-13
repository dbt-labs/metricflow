from __future__ import annotations

import itertools
import logging

from metricflow_semantic_interfaces.references import (
    DimensionReference,
    ElementReference,
    EntityReference,
    GroupByMetricReference,
    LinkableElementReference,
    MeasureReference,
    MetricModelReference,
    MetricReference,
    ModelReference,
    SemanticModelElementReference,
    SemanticModelReference,
    TimeDimensionReference,
)
from metricflow_semantic_interfaces.test_helpers.dataclass_serialization import (
    assert_includes_all_serializable_dataclass_types,
    assert_serializable,
)

from tests.test_dataclass_serialization import (
    DataclassWithDataclassDefault,
    DataclassWithDefaultTuple,
    DataclassWithOptional,
    DataclassWithPrimitiveTypes,
    DataclassWithTuple,
    DeeplyNestedDataclass,
    NestedDataclass,
    NestedDataclassWithProtocol,
    SimpleClassWithProtocol,
    SimpleDataclass,
)

logger = logging.getLogger(__name__)


def test_serializable_dataclass_subclasses() -> None:
    """Verify that all subclasses of `SerializableDataclass` are serializable."""
    counter = itertools.count(start=0)

    def _get_next_field_str() -> str:
        return f"field_{next(counter)}"

    instances = [
        LinkableElementReference(_get_next_field_str()),
        ElementReference(_get_next_field_str()),
        SemanticModelElementReference(_get_next_field_str(), _get_next_field_str()),
        EntityReference(_get_next_field_str()),
        SemanticModelReference(_get_next_field_str()),
        TimeDimensionReference(_get_next_field_str()),
        MetricReference(_get_next_field_str()),
        GroupByMetricReference(_get_next_field_str()),
        MetricModelReference(_get_next_field_str()),
        DimensionReference(_get_next_field_str()),
        MeasureReference(_get_next_field_str()),
        ModelReference(),
    ]

    assert_includes_all_serializable_dataclass_types(
        instances=instances,
        # These are classes defined and used in a separate test.
        excluded_classes=[
            DataclassWithDataclassDefault,
            DataclassWithDefaultTuple,
            DataclassWithOptional,
            DataclassWithPrimitiveTypes,
            DataclassWithTuple,
            DeeplyNestedDataclass,
            NestedDataclass,
            NestedDataclassWithProtocol,
            SimpleClassWithProtocol,
            SimpleDataclass,
        ],
    )
    assert_serializable(instances)
