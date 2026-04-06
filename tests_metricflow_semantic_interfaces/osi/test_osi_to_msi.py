"""Tests for OSIToMSIConverter."""

from __future__ import annotations

from metricflow.converters.osi.msi_to_osi import MSIToOSIConverter
from metricflow.converters.osi.osi_to_msi import OSIToMSIConverter
from metricflow_semantic_interfaces.implementations.semantic_model import PydanticNodeRelation
from metricflow_semantic_interfaces.test_utils import semantic_model_with_guaranteed_meta
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)
from tests_metricflow_semantic_interfaces.osi.helpers import (
    _dimension,
    _entity,
    _manifest,
    _measure,
    _osi_dataset,
    _osi_doc,
    _osi_field,
    _osi_metric,
    _osi_relationship,
    _simple_metric,
)


class TestOSIToMSIBasicConversion:  # noqa: D101
    def test_empty_document_produces_empty_manifest(self) -> None:  # noqa: D102
        result = OSIToMSIConverter().convert(_osi_doc())

        assert result.semantic_models == []
        assert result.metrics == []

    def test_single_dataset_becomes_semantic_model(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders", source="analytics.orders_table")])
        result = OSIToMSIConverter().convert(doc)

        assert len(result.semantic_models) == 1
        sm = result.semantic_models[0]
        assert sm.name == "orders"
        assert sm.node_relation.schema_name == "analytics"
        assert sm.node_relation.alias == "orders_table"
        assert sm.node_relation.database is None

    def test_description_carried_over(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders", description="Order data")])
        result = OSIToMSIConverter().convert(doc)

        assert result.semantic_models[0].description == "Order data"

    def test_source_two_parts(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("t", source="myschema.mytable")])
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert sm.node_relation.schema_name == "myschema"
        assert sm.node_relation.alias == "mytable"
        assert sm.node_relation.database is None

    def test_source_three_parts(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("t", source="mydb.myschema.mytable")])
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert sm.node_relation.database == "mydb"
        assert sm.node_relation.schema_name == "myschema"
        assert sm.node_relation.alias == "mytable"

    def test_source_bare_name(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("t", source="mytable")])
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert sm.node_relation.alias == "mytable"
        assert sm.node_relation.schema_name == ""

    def test_multiple_datasets_become_multiple_models(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders"), _osi_dataset("users")])
        result = OSIToMSIConverter().convert(doc)

        names = [sm.name for sm in result.semantic_models]
        assert names == ["orders", "users"]


class TestOSIToMSIFieldClassification:  # noqa: D101
    def test_primary_key_field_becomes_primary_entity(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset(
                    "orders",
                    fields=[_osi_field("order_id")],
                    primary_key=["order_id"],
                )
            ]
        )
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert len(sm.entities) == 1
        assert sm.entities[0].name == "order_id"
        assert sm.entities[0].type.value == "primary"

    def test_unique_key_field_becomes_unique_entity(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset(
                    "users",
                    fields=[_osi_field("email")],
                    unique_keys=[["email"]],
                )
            ]
        )
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert len(sm.entities) == 1
        assert sm.entities[0].name == "email"
        assert sm.entities[0].type.value == "unique"

    def test_relationship_from_column_becomes_foreign_entity(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset("orders", fields=[_osi_field("user_id")]),
                _osi_dataset("users", primary_key=["user_id"]),
            ],
            relationships=[_osi_relationship("r", "orders", "users", ["user_id"], ["user_id"])],
        )
        orders_sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert len(orders_sm.entities) == 1
        assert orders_sm.entities[0].name == "user_id"
        assert orders_sm.entities[0].type.value == "foreign"

    def test_is_time_true_becomes_time_dimension(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders", fields=[_osi_field("created_at", is_time=True)])])
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert len(sm.dimensions) == 1
        dim = sm.dimensions[0]
        assert dim.name == "created_at"
        assert dim.type == DimensionType.TIME
        assert dim.type_params is not None

    def test_is_time_false_becomes_categorical_dimension(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders", fields=[_osi_field("status", is_time=False)])])
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert len(sm.dimensions) == 1
        assert sm.dimensions[0].name == "status"
        assert sm.dimensions[0].type == DimensionType.CATEGORICAL

    def test_unmarked_field_becomes_categorical_dimension(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders", fields=[_osi_field("region")])])
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert len(sm.dimensions) == 1
        assert sm.dimensions[0].name == "region"

    def test_field_referenced_in_metric_becomes_measure(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("revenue", "SUM(amount)")],
        )
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert len(sm.measures) == 1
        assert sm.measures[0].name == "amount"
        assert sm.measures[0].agg == AggregationType.SUM

    def test_expr_different_from_name_is_preserved(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset(
                    "orders",
                    fields=[_osi_field("order_id", expression="id")],
                    primary_key=["order_id"],
                )
            ]
        )
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert sm.entities[0].expr == "id"

    def test_expr_same_as_name_is_stored_as_none(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset(
                    "orders",
                    fields=[_osi_field("order_id")],
                    primary_key=["order_id"],
                )
            ]
        )
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert sm.entities[0].expr is None

    def test_description_and_label_carried_over_to_dimension(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset(
                    "orders",
                    fields=[_osi_field("status", description="Order status", label="Status")],
                )
            ]
        )
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        dim = sm.dimensions[0]
        assert dim.description == "Order status"
        assert dim.label == "Status"


class TestOSIToMSIMetricConversion:  # noqa: D101
    def test_sum_expression_produces_simple_metric(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("revenue", "SUM(amount)")],
        )
        result = OSIToMSIConverter().convert(doc)

        assert len(result.metrics) == 1
        m = result.metrics[0]
        assert m.name == "revenue"
        assert m.type == MetricType.SIMPLE
        assert m.type_params.measure is not None
        assert m.type_params.measure.name == "amount"

    def test_count_distinct_expression(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("user_id")])],
            metrics=[_osi_metric("unique_users", "COUNT(DISTINCT user_id)")],
        )
        result = OSIToMSIConverter().convert(doc)

        m = result.metrics[0]
        assert m.type_params.measure is not None
        assert m.type_params.measure.name == "user_id"
        sm = result.semantic_models[0]
        assert sm.measures[0].agg.value == "count_distinct"

    def test_ratio_expression_produces_ratio_metric(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset(
                    "orders",
                    fields=[_osi_field("amount"), _osi_field("order_id")],
                )
            ],
            metrics=[_osi_metric("arpu", "(SUM(amount)) / (COUNT(order_id))")],
        )
        result = OSIToMSIConverter().convert(doc)

        ratio = next(m for m in result.metrics if m.type == MetricType.RATIO)
        assert ratio.name == "arpu"
        assert ratio.type_params.numerator is not None
        assert ratio.type_params.denominator is not None
        assert ratio.type_params.numerator.name == "arpu__numerator"
        assert ratio.type_params.denominator.name == "arpu__denominator"

    def test_ratio_sub_metrics_are_simple(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount"), _osi_field("cnt")])],
            metrics=[_osi_metric("ratio", "(SUM(amount)) / (COUNT(cnt))")],
        )
        result = OSIToMSIConverter().convert(doc)

        simple_metrics = [m for m in result.metrics if m.type == MetricType.SIMPLE]
        assert len(simple_metrics) == 2
        names = {m.name for m in simple_metrics}
        assert names == {"ratio__numerator", "ratio__denominator"}

    def test_complex_expression_falls_back_to_simple_with_synthetic_measure(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders")],
            metrics=[_osi_metric("complex", "SUM(a) + SUM(b)")],
        )
        result = OSIToMSIConverter().convert(doc)

        assert len(result.metrics) == 1
        m = result.metrics[0]
        assert m.type == MetricType.SIMPLE
        assert m.type_params.measure is not None
        assert m.type_params.measure.name == "complex__expr"

    def test_metric_description_carried_over(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("revenue", "SUM(amount)", description="Total revenue")],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.metrics[0].description == "Total revenue"

    def test_no_metrics_produces_empty_list(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders")])
        result = OSIToMSIConverter().convert(doc)

        assert result.metrics == []

    def test_dataset_qualified_column_reference(self) -> None:  # noqa: D102
        """A metric referencing 'dataset.col' should classify 'col' as a measure in that dataset."""
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("revenue", "SUM(orders.amount)")],
        )
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert len(sm.measures) == 1
        assert sm.measures[0].name == "amount"


class TestOSIToMSIRoundTrip:  # noqa: D101
    def test_field_names_and_counts_survive_round_trip(self) -> None:  # noqa: D102
        """MSI → OSI → MSI preserves dataset names, field counts, and entity types."""
        original = _manifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="orders",
                    node_relation=PydanticNodeRelation(schema_name="analytics", alias="orders"),
                    entities=[_entity("order_id", entity_type=EntityType.PRIMARY)],
                    dimensions=[
                        _dimension("status"),
                        _dimension("created_at", dim_type=DimensionType.TIME, granularity=TimeGranularity.DAY),
                    ],
                    measures=[_measure("revenue", agg=AggregationType.SUM, expr="amount")],
                )
            ],
            metrics=[_simple_metric("revenue", "revenue")],
        )

        osi_doc = MSIToOSIConverter().convert(original, model_name="my_model")
        recovered = OSIToMSIConverter().convert(osi_doc)

        assert len(recovered.semantic_models) == 1
        sm = recovered.semantic_models[0]
        assert sm.name == "orders"

        entity_names = {e.name for e in sm.entities}
        assert "order_id" in entity_names

        dim_names = {d.name for d in sm.dimensions}
        assert "status" in dim_names
        assert "created_at" in dim_names

        measure_names = {m.name for m in sm.measures}
        assert "revenue" in measure_names

        assert len(recovered.metrics) == 1
        assert recovered.metrics[0].name == "revenue"
