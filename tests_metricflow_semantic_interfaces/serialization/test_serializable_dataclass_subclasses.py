from __future__ import annotations

import itertools
import logging

from metricflow_semantic_interfaces.dataclass_serialization import SerializableDataclass
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
from tests_metricflow_semantic_interfaces.test_dataclass_serialization import (
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
        excluded_classes=[
            # These are classes defined and used in a separate test.
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
            # Exclude SerializableDataclass subclasses from other packages — the registry
            # is global, so classes outside metricflow_semantic_interfaces may appear when
            # running in a combined environment.
            *(
                cls
                for cls in SerializableDataclass.concrete_subclasses_for_testing()
                if not cls.__module__.startswith("metricflow_semantic_interfaces")
                and not cls.__module__.startswith("tests_metricflow_semantic_interfaces")
            ),
        ],
    )
    assert_serializable(instances)
