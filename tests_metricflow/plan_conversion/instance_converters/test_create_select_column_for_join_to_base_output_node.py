from __future__ import annotations

from typing import Mapping, Type

import pytest
from metricflow_semantics.instances import InstanceSet
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.sql.sql_exprs import SqlAggregateFunctionExpression, SqlColumnReferenceExpression

from metricflow.plan_conversion.instance_set_transforms.instance_converters import (
    CreateSelectColumnForCombineOutputNode,
    CreateSelectColumnForJoinToBaseOutputNode,
    SelectElementsTransform,
)
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup


@pytest.mark.parametrize(
    ("transform_class", "expected_expr_class"),
    (
        (CreateSelectColumnForCombineOutputNode, SqlAggregateFunctionExpression),
        (CreateSelectColumnForJoinToBaseOutputNode, SqlColumnReferenceExpression),
    ),
)
def test_simple_metric_input_column_expression(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    transform_class: Type[CreateSelectColumnForCombineOutputNode],
    expected_expr_class: type,
) -> None:
    """Combining re-aggregates the input column with MAX; joining to the base leaves it as is."""
    instance_set = (
        mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]
        .data_set_mapping["bookings_source"]
        .instance_set
    )
    include_specs = tuple(
        instance.spec
        for instance in instance_set.simple_metric_input_instances
        if instance.spec.element_name == "booking_value"
    )
    filtered_instance_set: InstanceSet = SelectElementsTransform(
        include_specs=InstanceSpecSet(simple_metric_input_specs=include_specs)
    ).transform(instance_set)

    select_column_set = transform_class(
        table_alias="a",
        column_resolver=DunderColumnAssociationResolver(),
        metric_lookup=simple_semantic_manifest_lookup.metric_lookup,
    ).transform(filtered_instance_set)

    assert len(select_column_set.simple_metric_input_columns) == 1
    assert type(select_column_set.simple_metric_input_columns[0].expr) is expected_expr_class
