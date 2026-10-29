"""Tests for OSIToMSIConverter."""

from __future__ import annotations

import pytest
import sqlglot
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.snapshot_helpers import (
    SnapshotConfiguration,
    assert_object_snapshot_equal,
)

from metricflow.converters.converter_issues import ConverterIssue, ConverterIssueType
from metricflow.converters.models import (
    OSIDataType,
    OSIDialect,
    OSIDialectExpression,
    OSIDocument,
    OSIExpression,
    OSIField,
    OSIMetric,
)
from metricflow.converters.msi_to_osi import MSIToOSIConverter
from metricflow.converters.osi_to_msi import OSIToMSIConverter
from metricflow_semantic_interfaces.implementations.elements.measure import (
    PydanticMeasureAggregationParameters,
)
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricAggregationParams,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.test_utils import default_meta, semantic_model_with_guaranteed_meta
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    DataType,
    DimensionType,
    MetricType,
)
from tests_metricflow_semantic_interfaces.osi.helpers import (
    _manifest,
    _measure,
    _osi_dataset,
    _osi_doc,
    _osi_field,
    _osi_metric,
    _osi_relationship,
)


def _agg_params(metric: PydanticMetric) -> PydanticMetricAggregationParams:
    """Return a SIMPLE metric's aggregation parameters, asserting they are set."""
    params = metric.type_params.metric_aggregation_params
    assert params is not None
    return params


def _only_metric(document: OSIDocument) -> OSIMetric:
    """Return the first OSI metric of the first semantic model, asserting it exists."""
    metrics = document.semantic_model[0].metrics
    assert metrics is not None
    return metrics[0]


class TestOSIToMSIBasicConversion:  # noqa: D101
    def test_empty_document_produces_empty_manifest(self) -> None:  # noqa: D102
        result = OSIToMSIConverter().convert(_osi_doc()).output

        assert result.semantic_models == []
        assert result.metrics == []

    def test_single_dataset_becomes_semantic_model(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders", source="analytics.orders_table")])
        result = OSIToMSIConverter().convert(doc).output

        assert len(result.semantic_models) == 1
        sm = result.semantic_models[0]
        assert sm.name == "orders"
        assert sm.node_relation.schema_name == "analytics"
        assert sm.node_relation.alias == "orders_table"
        assert sm.node_relation.database is None

    def test_description_carried_over(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders", description="Order data")])
        result = OSIToMSIConverter().convert(doc).output

        assert result.semantic_models[0].description == "Order data"

    def test_source_two_parts(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("t", source="myschema.mytable")])
        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

        assert sm.node_relation.schema_name == "myschema"
        assert sm.node_relation.alias == "mytable"
        assert sm.node_relation.database is None

    def test_source_three_parts(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("t", source="mydb.myschema.mytable")])
        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

        assert sm.node_relation.database == "mydb"
        assert sm.node_relation.schema_name == "myschema"
        assert sm.node_relation.alias == "mytable"

    def test_source_bare_name(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("t", source="mytable")])
        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

        assert sm.node_relation.alias == "mytable"
        assert sm.node_relation.schema_name == ""

    def test_multiple_datasets_become_multiple_models(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders"), _osi_dataset("users")])
        result = OSIToMSIConverter().convert(doc).output

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
        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

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
        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

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
        orders_sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

        assert len(orders_sm.entities) == 1
        assert orders_sm.entities[0].name == "user_id"
        assert orders_sm.entities[0].type.value == "foreign"

    @pytest.mark.parametrize(
        "field_name, is_time, expected_type",
        [
            ("created_at", True, DimensionType.TIME),
            ("status", False, DimensionType.CATEGORICAL),
            ("region", None, DimensionType.CATEGORICAL),
        ],
    )
    def test_field_becomes_dimension_by_is_time(  # noqa: D102
        self, field_name: str, is_time: bool | None, expected_type: DimensionType
    ) -> None:
        doc = _osi_doc(datasets=[_osi_dataset("orders", fields=[_osi_field(field_name, is_time=is_time)])])
        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

        assert len(sm.dimensions) == 1
        assert sm.dimensions[0].name == field_name
        assert sm.dimensions[0].type == expected_type

    def test_field_referenced_in_metric_stays_as_dimension(self) -> None:  # noqa: D102
        """Fields referenced in metric expressions are no longer promoted to measures."""
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("revenue", "SUM(amount)")],
        )
        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

        assert len(sm.measures) == 0
        assert len(sm.dimensions) == 1
        assert sm.dimensions[0].name == "amount"

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
        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

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
        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

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
        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

        dim = sm.dimensions[0]
        assert dim.description == "Order status"
        assert dim.label == "Status"

    def test_datatype_carried_over_to_categorical_dimension(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders", fields=[_osi_field("status", datatype=OSIDataType.STRING)])])
        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

        assert sm.dimensions[0].datatype is DataType.STRING

    def test_datatype_carried_over_to_time_dimension(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset(
                    "orders",
                    fields=[_osi_field("created_at", is_time=True, datatype=OSIDataType.DATE_TIME_TZ)],
                )
            ]
        )
        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

        assert sm.dimensions[0].datatype is DataType.DATETIME_TZ

    def test_missing_datatype_on_dimension_is_none(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders", fields=[_osi_field("status")])])
        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

        assert sm.dimensions[0].datatype is None

    @pytest.mark.parametrize(
        ("primary_key", "unique_keys", "expected_error"),
        [
            (
                ["tenant_id", "order_id"],
                None,
                "Dataset 'orders' has composite primary key ['tenant_id', 'order_id']; "
                "MetricFlow entities cannot represent composite keys losslessly",
            ),
            (
                None,
                [["tenant_id", "external_id"]],
                "Dataset 'orders' has composite unique key ['tenant_id', 'external_id']; "
                "MetricFlow entities cannot represent composite keys losslessly",
            ),
        ],
    )
    def test_composite_key_is_rejected(  # noqa: D102
        self,
        primary_key: list[str] | None,
        unique_keys: list[list[str]] | None,
        expected_error: str,
    ) -> None:
        doc = _osi_doc(
            datasets=[
                _osi_dataset(
                    "orders",
                    fields=[_osi_field("tenant_id"), _osi_field("order_id")],
                    primary_key=primary_key,
                    unique_keys=unique_keys,
                )
            ]
        )

        with pytest.raises(ValueError) as exc_info:
            OSIToMSIConverter().convert(doc)

        assert str(exc_info.value) == expected_error


class TestOSIToMSIMetricConversion:  # noqa: D101
    def test_sum_expression_produces_simple_metric(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("revenue", "SUM(amount)")],
        )
        result = OSIToMSIConverter().convert(doc).output

        assert len(result.metrics) == 1
        m = result.metrics[0]
        assert m.name == "revenue"
        assert m.type == MetricType.SIMPLE
        assert m.type_params.measure is None
        assert m.type_params.metric_aggregation_params is not None
        assert _agg_params(m).agg == AggregationType.SUM
        assert m.type_params.expr == "amount"
        assert _agg_params(m).semantic_model == "orders"

    def test_count_distinct_expression(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("user_id")])],
            metrics=[_osi_metric("unique_users", "COUNT(DISTINCT user_id)")],
        )
        result = OSIToMSIConverter().convert(doc).output

        m = result.metrics[0]
        assert m.type_params.measure is None
        assert m.type_params.metric_aggregation_params is not None
        assert _agg_params(m).agg == AggregationType.COUNT_DISTINCT
        assert m.type_params.expr == "user_id"
        sm = result.semantic_models[0]
        assert len(sm.measures) == 0

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
        result = OSIToMSIConverter().convert(doc).output

        ratio = next(m for m in result.metrics if m.type == MetricType.RATIO)
        assert ratio.name == "arpu"
        assert ratio.type_params.numerator is not None
        assert ratio.type_params.denominator is not None
        assert ratio.type_params.numerator.name == "arpu_numerator"
        assert ratio.type_params.denominator.name == "arpu_denominator"

    def test_ratio_sub_metrics_are_simple(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount"), _osi_field("cnt")])],
            metrics=[_osi_metric("ratio", "(SUM(amount)) / (COUNT(cnt))")],
        )
        result = OSIToMSIConverter().convert(doc).output

        simple_metrics = [m for m in result.metrics if m.type == MetricType.SIMPLE]
        assert len(simple_metrics) == 2
        names = {m.name for m in simple_metrics}
        assert names == {"ratio_numerator", "ratio_denominator"}

    def test_metric_description_carried_over(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("revenue", "SUM(amount)", description="Total revenue")],
        )
        result = OSIToMSIConverter().convert(doc).output

        assert result.metrics[0].description == "Total revenue"

    def test_simple_metric_datatype_carried_over(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("revenue", "SUM(amount)", datatype=OSIDataType.DECIMAL)],
        )
        result = OSIToMSIConverter().convert(doc).output

        assert result.metrics[0].datatype is DataType.DECIMAL

    def test_missing_datatype_on_metric_is_none(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("revenue", "SUM(amount)")],
        )
        result = OSIToMSIConverter().convert(doc).output

        assert result.metrics[0].datatype is None

    def test_ratio_metric_datatype_not_propagated_to_sub_metrics(self) -> None:  # noqa: D102
        """Only the RATIO metric itself carries the OSI metric's datatype, not its auto-generated sub-metrics."""
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount"), _osi_field("cnt")])],
            metrics=[_osi_metric("arpu", "(SUM(amount)) / (COUNT(cnt))", datatype=OSIDataType.FLOAT)],
        )
        result = OSIToMSIConverter().convert(doc).output

        ratio = next(m for m in result.metrics if m.type == MetricType.RATIO)
        sub_metrics = [m for m in result.metrics if m.type == MetricType.SIMPLE]
        assert ratio.datatype is DataType.FLOAT
        assert all(m.datatype is None for m in sub_metrics)

    def test_no_metrics_produces_empty_list(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders")])
        result = OSIToMSIConverter().convert(doc).output

        assert result.metrics == []

    def test_dataset_qualified_column_reference(self) -> None:  # noqa: D102
        """A metric referencing 'dataset.col' should resolve the semantic_model to that dataset."""
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("revenue", "SUM(orders.amount)")],
        )
        result = OSIToMSIConverter().convert(doc).output

        m = result.metrics[0]
        assert m.type_params.metric_aggregation_params is not None
        assert _agg_params(m).semantic_model == "orders"
        assert m.type_params.expr == "amount"

    def test_percentile_cont_0_5_produces_median(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("median_amount", "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)")],
        )
        result = OSIToMSIConverter().convert(doc).output

        m = result.metrics[0]
        assert m.type_params.metric_aggregation_params is not None
        assert _agg_params(m).agg == AggregationType.MEDIAN
        assert _agg_params(m).agg_params is None
        assert m.type_params.expr == "amount"

    def test_percentile_cont_non_median_carries_percentile_param(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("p95_amount", "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount)")],
        )
        result = OSIToMSIConverter().convert(doc).output

        m = result.metrics[0]
        assert m.type_params.metric_aggregation_params is not None
        assert m.type_params.metric_aggregation_params.agg == AggregationType.PERCENTILE
        assert m.type_params.metric_aggregation_params.agg_params is not None
        assert m.type_params.metric_aggregation_params.agg_params.percentile == 0.95
        assert m.type_params.expr == "amount"

    @pytest.mark.parametrize("expression", ["COUNT(*)", "COUNT(orders.*)", "COUNT(2)", "COUNT(TRUE)", "COUNT(0)"])
    def test_count_of_a_constant_normalizes_to_row_count_expr(self, expression: str) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("order_id")])],
            metrics=[_osi_metric("order_count", expression)],
        )
        result = OSIToMSIConverter().convert(doc).output

        m = result.metrics[0]
        assert m.type_params.metric_aggregation_params is not None
        assert _agg_params(m).agg == AggregationType.COUNT
        assert m.type_params.expr == "1"

    @pytest.mark.parametrize(("expression", "expr"), [("SUM(1)", "1"), ("SUM(2)", "2"), ("SUM(TRUE)", "TRUE")])
    def test_sum_of_a_constant_keeps_its_value(self, expression: str, expr: str) -> None:
        """SUM adds the constant up, so SUM(2) is twice the row count and must not become SUM(1)."""
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("order_id")])],
            metrics=[_osi_metric("total", expression)],
        )
        result = OSIToMSIConverter().convert(doc).output

        m = result.metrics[0]
        assert m.type_params.metric_aggregation_params is not None
        assert _agg_params(m).agg == AggregationType.SUM
        assert _agg_params(m).semantic_model == "orders"
        assert m.type_params.expr == expr

    @pytest.mark.parametrize("expression", ["SUM(1)", "SUM(2)", "SUM(0)"])
    def test_sum_of_a_constant_with_multiple_datasets_is_dropped_with_a_warning(self, expression: str) -> None:
        """A constant has no column to place it in a dataset, so with several it is refused, not guessed."""
        doc = _osi_doc(datasets=self._customers_and_orders(), metrics=[_osi_metric("total", expression)])
        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert [i.element_name for i in result.issues] == ["total"]

    def test_qualified_count_star_uses_dataset_qualifier(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset("customers", fields=[_osi_field("customer_id")]),
                _osi_dataset("orders", fields=[_osi_field("order_id")]),
            ],
            metrics=[_osi_metric("order_count", "COUNT(orders.*)")],
        )
        result = OSIToMSIConverter().convert(doc).output

        m = result.metrics[0]
        assert m.type_params.expr == "1"
        assert m.type_params.metric_aggregation_params is not None
        assert _agg_params(m).semantic_model == "orders"

    @staticmethod
    def _customers_and_orders() -> list:
        return [
            _osi_dataset("customers", fields=[_osi_field("customer_id")]),
            _osi_dataset("orders", fields=[_osi_field("order_id"), _osi_field("amount")]),
        ]

    @pytest.mark.parametrize("expression", ["COUNT(*)", "COUNT(1)", "COUNT(2)", "COUNT(0)", "COUNT(TRUE)"])
    def test_bare_row_count_with_multiple_datasets_is_dropped_with_a_warning(  # noqa: D102
        self, expression: str
    ) -> None:
        doc = _osi_doc(datasets=self._customers_and_orders(), metrics=[_osi_metric("order_count", expression)])
        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert result.issues == [ConverterIssue(ConverterIssueType.ROW_COUNT_METRIC_DROPPED, "order_count")]

    def test_ratio_with_a_bare_count_star_is_dropped_as_a_whole(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=self._customers_and_orders(),
            metrics=[
                _osi_metric("revenue", "SUM(orders.amount)"),
                _osi_metric("avg_order_value", "(SUM(orders.amount)) / (COUNT(*))"),
            ],
        )
        result = OSIToMSIConverter().convert(doc)

        assert [m.name for m in result.output.metrics] == ["revenue"]
        assert result.issues == [ConverterIssue(ConverterIssueType.ROW_COUNT_METRIC_DROPPED, "avg_order_value")]

    def test_ratio_with_a_distinct_row_count_is_dropped_as_a_whole(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=self._customers_and_orders(),
            metrics=[
                _osi_metric("revenue", "SUM(orders.amount)"),
                _osi_metric("avg_order_value", "(SUM(orders.amount)) / (COUNT(DISTINCT *))"),
            ],
        )
        result = OSIToMSIConverter().convert(doc)

        assert [m.name for m in result.output.metrics] == ["revenue"]
        assert result.issues == [ConverterIssue(ConverterIssueType.ROW_COUNT_METRIC_DROPPED, "avg_order_value")]

    def test_qualified_count_star_in_ratio_binds_both_sides_to_the_same_dataset(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=self._customers_and_orders(),
            metrics=[_osi_metric("avg_order_value", "(SUM(orders.amount)) / (COUNT(orders.*))")],
        )
        result = OSIToMSIConverter().convert(doc).output

        by_name = {m.name: m for m in result.metrics}
        for sub_metric in ("avg_order_value_numerator", "avg_order_value_denominator"):
            params = by_name[sub_metric].type_params.metric_aggregation_params
            assert params is not None
            assert params.semantic_model == "orders"

    def test_schema_qualified_count_star_matches_dataset_by_last_segment(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=self._customers_and_orders(),
            metrics=[_osi_metric("order_count", "COUNT(db.orders.*)")],
        )
        result = OSIToMSIConverter().convert(doc).output

        params = result.metrics[0].type_params.metric_aggregation_params
        assert params is not None
        assert params.semantic_model == "orders"

    def test_count_star_of_unknown_dataset_is_dropped_with_a_warning(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=self._customers_and_orders(),
            metrics=[_osi_metric("order_count", "COUNT(nope.*)")],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert [i.element_name for i in result.issues] == ["order_count"]

    def test_count_star_matching_several_datasets_by_last_segment_is_dropped_with_a_warning(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset("a.orders", fields=[_osi_field("order_id")]),
                _osi_dataset("b.orders", fields=[_osi_field("order_id")]),
            ],
            metrics=[_osi_metric("order_count", "COUNT(orders.*)")],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert [i.element_name for i in result.issues] == ["order_count"]

    @pytest.mark.parametrize(
        "expression",
        [
            "COUNT(DISTINCT *)",
            "COUNT(DISTINCT 1)",
            "COUNT(DISTINCT orders.*)",
            "COUNT(DISTINCT (*))",
            "COUNT(DISTINCT (1))",
            "(COUNT(DISTINCT *))",
            "COUNT(DISTINCT *) * 100",
            "COALESCE(COUNT(DISTINCT 1), 0)",
            "CAST(COUNT(DISTINCT *) AS INT)",
        ],
    )
    def test_distinct_of_a_row_count_is_dropped_with_a_warning(self, expression: str) -> None:
        """COUNT(DISTINCT *) and friends are dropped with a warning.

        They are valid SQL but answer 'does any row exist' (0 or 1), not a
        meaningful total. Falling back to a raw SUM of it would be a nested aggregate MetricFlow cannot
        run, bound to a guessed dataset; dropped instead, the same as an unresolvable COUNT(*). Caught
        whether it is the whole expression, redundantly parenthesized, or wrapped in another function.
        """
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("order_id"), _osi_field("amount")])],
            metrics=[_osi_metric("odd_count", expression)],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert [i.element_name for i in result.issues] == ["odd_count"]

    def test_sum_boolean_qualified_column_resolves_semantic_model(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset("customers", fields=[_osi_field("customer_id")]),
                _osi_dataset("orders", fields=[_osi_field("order_id")]),
            ],
            metrics=[
                _osi_metric(
                    "has_order",
                    "SUM(CASE WHEN orders.order_id IS NOT NULL THEN 1 ELSE 0 END)",
                )
            ],
        )
        result = OSIToMSIConverter().convert(doc).output

        metric = result.metrics[0]
        assert metric.type_params.metric_aggregation_params is not None
        assert _agg_params(metric).agg == AggregationType.SUM_BOOLEAN
        assert _agg_params(metric).semantic_model == "orders"

    def test_fully_qualified_column_preserves_dataset_name(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset(
                    "analytics.orders",
                    fields=[_osi_field("order_id")],
                )
            ],
            metrics=[_osi_metric("order_count", "COUNT(analytics.orders.order_id)")],
        )
        result = OSIToMSIConverter().convert(doc).output

        metric = result.metrics[0]
        assert metric.type_params.metric_aggregation_params is not None
        assert _agg_params(metric).semantic_model == "analytics.orders"

    @pytest.mark.parametrize(
        ("expression", "expected_agg", "expected_expr"),
        [
            ("SUM(orders.gross - orders.tax)", AggregationType.SUM, "gross - tax"),
            ("SUM(COALESCE(orders.tax, 0))", AggregationType.SUM, "COALESCE(tax, 0)"),
            ("MAX(orders.gross - orders.tax)", AggregationType.MAX, "gross - tax"),
            ("SUM(amount * 0.5)", AggregationType.SUM, "amount * 0.5"),
            ("AVG(CAST(orders.tax AS DOUBLE))", AggregationType.AVERAGE, "CAST(tax AS DOUBLE)"),
            (
                "COUNT(DISTINCT orders.status || orders.region)",
                AggregationType.COUNT_DISTINCT,
                "status || region",
            ),
            (
                "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY orders.gross - orders.tax)",
                AggregationType.PERCENTILE,
                "gross - tax",
            ),
        ],
    )
    def test_compound_aggregate_argument_is_unqualified_as_a_whole(
        self, expression: str, expected_agg: AggregationType, expected_expr: str
    ) -> None:
        """Every column reference inside the aggregate argument loses its dataset qualifier.

        The surrounding expression is kept intact rather than sliced at the last dot.
        """
        doc = _osi_doc(
            datasets=[
                _osi_dataset(
                    "orders",
                    fields=[
                        _osi_field("amount"),
                        _osi_field("gross"),
                        _osi_field("tax"),
                        _osi_field("status"),
                        _osi_field("region"),
                    ],
                )
            ],
            metrics=[_osi_metric("m", expression)],
        )
        result = OSIToMSIConverter().convert(doc).output

        m = result.metrics[0]
        assert m.type_params.metric_aggregation_params is not None
        assert _agg_params(m).agg == expected_agg
        assert _agg_params(m).semantic_model == "orders"
        assert m.type_params.expr == expected_expr

    def test_unparseable_expression_is_dropped_with_an_issue(self) -> None:  # noqa: D102
        expression = "SUM(CASE WHEN status = 'paid THEN amount END)"
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("paid", expression)],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert result.issues == [ConverterIssue(ConverterIssueType.UNSUPPORTED_METRIC_EXPRESSION, "paid")]

    @pytest.mark.parametrize("expression", ["COUNT(orders.*, amount)", "COUNT(db.orders, *)"])
    def test_unsupported_count_star_forms_are_dropped_with_an_issue(self, expression: str) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("order_id"), _osi_field("amount")])],
            metrics=[_osi_metric("odd_count", expression)],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert result.issues == [ConverterIssue(ConverterIssueType.UNSUPPORTED_METRIC_EXPRESSION, "odd_count")]

    def test_composite_aggregate_expression_is_dropped_with_issue(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders")],
            metrics=[_osi_metric("complex", "SUM(a) + SUM(b)")],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert result.issues == [ConverterIssue(ConverterIssueType.UNSUPPORTED_METRIC_EXPRESSION, "complex")]

    @pytest.mark.parametrize(
        "expression",
        [
            "price * quantity",
            "SUM(MAX(amount))",
            "SUM(DISTINCT amount)",
            "SUM(MY_UDAF(amount))",
            "SUM((SELECT x FROM other_table LIMIT 1))",
        ],
    )
    def test_unsafe_or_non_aggregate_expression_is_dropped_with_issue(self, expression: str) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("unsupported", expression)],
        )

        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert result.issues == [ConverterIssue(ConverterIssueType.UNSUPPORTED_METRIC_EXPRESSION, "unsupported")]

    def test_compound_scalar_expression_cannot_span_semantic_models(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset("orders", fields=[_osi_field("amount")]),
                _osi_dataset("refunds", fields=[_osi_field("amount")]),
            ],
            metrics=[
                _osi_metric(
                    "net_amount",
                    "SUM(orders.amount + refunds.amount)",
                )
            ],
        )

        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert result.issues == [ConverterIssue(ConverterIssueType.UNSUPPORTED_METRIC_EXPRESSION, "net_amount")]

    def test_dax_measure_is_not_reinterpreted_as_sql_simple_metric(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("Sales")],
            metrics=[
                _osi_metric(
                    "Revenue",
                    "SUM(Sales[Amount])",
                    dialect=OSIDialect.DAX,
                )
            ],
        )

        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert result.issues == [ConverterIssue(ConverterIssueType.UNSUPPORTED_METRIC_EXPRESSION, "Revenue")]


class TestOSIToMSIRoundTrip:  # noqa: D101
    def test_osi_to_msi_to_osi_preserves_structure(  # noqa: D102
        self, request: FixtureRequest, snapshot_configuration: SnapshotConfiguration
    ) -> None:
        """OSI → MSI → OSI preserves dataset names, fields, and metric expressions."""
        original = _osi_doc(
            datasets=[
                _osi_dataset(
                    "orders",
                    source="analytics.orders",
                    fields=[
                        _osi_field("order_id"),
                        _osi_field("status"),
                        _osi_field("created_at", is_time=True),
                        _osi_field("amount"),
                    ],
                    primary_key=["order_id"],
                )
            ],
            metrics=[_osi_metric("revenue", "SUM(orders.amount)")],
        )

        msi = OSIToMSIConverter().convert(original).output
        assert msi.semantic_models[0].measures == []

        osi_doc = MSIToOSIConverter().convert(msi).output

        dataset = osi_doc.semantic_model[0].datasets[0]
        assert dataset.name == "orders"

        field_names = {f.name for f in dataset.fields or []}
        assert "order_id" in field_names
        assert "status" in field_names
        assert "created_at" in field_names
        assert "amount" in field_names

        metrics = osi_doc.semantic_model[0].metrics or []
        assert len(metrics) == 1
        assert metrics[0].name == "revenue"
        assert metrics[0].expression.dialects[0].expression == "SUM(orders.amount)"
        assert_object_snapshot_equal(
            request=request,
            snapshot_configuration=snapshot_configuration,
            obj=osi_doc,
        )

    def test_datatype_round_trips_through_osi_to_msi_to_osi(self) -> None:  # noqa: D102
        """An OSI Field/Metric.datatype value survives an OSI -> MSI -> OSI round trip unchanged."""
        original = _osi_doc(
            datasets=[
                _osi_dataset(
                    "orders",
                    fields=[
                        _osi_field("status", datatype=OSIDataType.STRING),
                        _osi_field("created_at", is_time=True, datatype=OSIDataType.DATE_TIME_TZ),
                        _osi_field("amount"),
                    ],
                )
            ],
            metrics=[_osi_metric("revenue", "SUM(amount)", datatype=OSIDataType.DECIMAL)],
        )

        msi = OSIToMSIConverter().convert(original).output
        osi_doc = MSIToOSIConverter().convert(msi).output

        fields = {f.name: f for f in osi_doc.semantic_model[0].datasets[0].fields or []}
        assert fields["status"].datatype is OSIDataType.STRING
        assert fields["created_at"].datatype is OSIDataType.DATE_TIME_TZ
        assert fields["amount"].datatype is None

        metrics = osi_doc.semantic_model[0].metrics or []
        assert len(metrics) == 1
        assert metrics[0].datatype is OSIDataType.DECIMAL

    def test_count_star_round_trips_to_a_portable_sum_one(self) -> None:
        """COUNT(orders.*) survives one round trip as the portable SUM(1), not the original invalid expr '*'.

        The dataset is not recovered on this leg: MSI has already collapsed the metric to agg=SUM, expr='1'
        by the time it reaches MSIToOSIConverter (MetricFlow's own transform runs first), and SUM(1) has
        no qualifier to recover a dataset from. Emitting COUNT(<dataset>.*) instead, to carry the dataset
        through, was tried and reverted: it is outside the OSI expression spec, several engines reject or
        misinterpret it, and no sibling converter recognizes it as a row count. SUM(1) is what every engine
        agrees on.

        Converting SUM(1) forward again, with more than one dataset, now refuses rather than silently
        picking the first dataset: SUM of a constant has no column to place it in a dataset, so it goes
        through the same ambiguity check as COUNT(*) and is dropped with ROW_COUNT_METRIC_DROPPED instead of
        a plausible but wrong semantic_model. The dataset is genuinely lost by this point, not recoverable; refusing is
        the honest outcome, not a bug to fix on the next leg.
        """
        original = _osi_doc(
            datasets=[
                _osi_dataset("customers", fields=[_osi_field("customer_id")]),
                _osi_dataset("orders", fields=[_osi_field("order_id"), _osi_field("amount")]),
            ],
            metrics=[
                _osi_metric("order_count", "COUNT(orders.*)"),
                _osi_metric("avg_order_value", "(SUM(orders.amount)) / (COUNT(orders.*))"),
            ],
        )

        msi = OSIToMSIConverter().convert(original).output
        osi_doc = MSIToOSIConverter().convert(msi).output

        expressions = {m.name: m.expression.dialects[0].expression for m in osi_doc.semantic_model[0].metrics or []}
        assert expressions["order_count"] == "SUM(1)"
        assert "SUM(1)" in expressions["avg_order_value"]

        # The first round trip already flattened the ratio into independent metrics (numerator,
        # denominator, and the ratio's own expression string). Only the ones that are still row
        # counts drop here: the denominator (SUM(1), ambiguous) and the ratio itself, which depends
        # on it. The numerator (SUM(orders.amount), not a row count) is a valid metric on its own
        # and survives, same as it would have before any of this.
        again = OSIToMSIConverter().convert(osi_doc)
        assert [m.name for m in again.output.metrics] == ["avg_order_value_numerator"]
        assert {i.element_name for i in again.issues} == {
            "order_count",
            "avg_order_value",
            "avg_order_value_denominator",
        }

    def test_discrete_percentile_survives_round_trip(self) -> None:
        """A PERCENTILE_DISC metric keeps use_discrete_percentile through MSI -> OSI -> MSI."""
        orders = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("amount", agg=AggregationType.SUM, expr="amount")],
        )
        metric = PydanticMetric(
            name="p95_amount",
            description=None,
            type=MetricType.SIMPLE,
            type_params=PydanticMetricTypeParams(
                expr="amount",
                metric_aggregation_params=PydanticMetricAggregationParams(
                    semantic_model="orders",
                    agg=AggregationType.PERCENTILE,
                    agg_params=PydanticMeasureAggregationParameters(
                        percentile=0.95,
                        use_discrete_percentile=True,
                    ),
                    agg_time_dimension=None,
                    non_additive_dimension=None,
                ),
            ),
            filter=None,
            metadata=default_meta(),
            config=None,
        )

        osi_doc = MSIToOSIConverter().convert(_manifest(semantic_models=[orders], metrics=[metric])).output

        osi_metrics = osi_doc.semantic_model[0].metrics
        assert osi_metrics is not None
        osi_expr = osi_metrics[0].expression.dialects[0].expression
        assert osi_expr == "PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY orders.amount)"

        back = OSIToMSIConverter().convert(osi_doc).output
        m = back.metrics[0]

        assert m.type_params.metric_aggregation_params is not None
        assert m.type_params.metric_aggregation_params.agg == AggregationType.PERCENTILE
        assert m.type_params.metric_aggregation_params.agg_params is not None
        assert m.type_params.metric_aggregation_params.agg_params.use_discrete_percentile is True

    def test_compound_aggregate_argument_survives_round_trip(self) -> None:
        """OSI → MSI → OSI keeps a compound aggregate argument intact."""
        original = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("gross"), _osi_field("tax")])],
            metrics=[
                _osi_metric("net_sales", "SUM(orders.gross - orders.tax)"),
                _osi_metric("tax_or_zero", "SUM(COALESCE(orders.tax, 0))"),
            ],
        )

        msi = OSIToMSIConverter().convert(original).output
        osi_doc = MSIToOSIConverter().convert(msi).output

        metrics = osi_doc.semantic_model[0].metrics or []
        expressions = {m.name: m.expression.dialects[0].expression for m in metrics}
        # msi_to_osi._qualify_col re-qualifies only bare identifiers,
        # so the exported argument comes back unqualified.
        assert expressions == {
            "net_sales": "SUM(gross - tax)",
            "tax_or_zero": "SUM(COALESCE(tax, 0))",
        }

    def test_sum_over_case_round_trip_does_not_double_aggregate(self) -> None:  # noqa: D102
        expression = "SUM(CASE WHEN orders.status = 'paid' THEN amount ELSE 0 END)"
        original = _osi_doc(
            datasets=[
                _osi_dataset(
                    "orders",
                    fields=[_osi_field("status"), _osi_field("amount")],
                )
            ],
            metrics=[_osi_metric("paid_revenue", expression)],
        )

        forward = OSIToMSIConverter().convert(original)

        assert forward.issues == []
        metric = forward.output.metrics[0]
        assert metric.type_params.metric_aggregation_params is not None
        assert _agg_params(metric).agg == AggregationType.SUM
        assert metric.type_params.expr == "CASE WHEN status = 'paid' THEN amount ELSE 0 END"

        backward = MSIToOSIConverter().convert(forward.output)

        assert backward.issues == []
        assert (
            _only_metric(backward.output).expression.dialects[0].expression
            == "SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END)"
        )


def _multi_dialect_expr(*pairs: tuple[OSIDialect, str]) -> OSIExpression:
    """Build an expression carrying more than one dialect, in the given order."""
    return OSIExpression(dialects=[OSIDialectExpression(dialect=dialect, expression=expr) for dialect, expr in pairs])


_SNOWFLAKE_METRIC = (OSIDialect.SNOWFLAKE, "SUM(orders.amt_snowflake_only)")
_OSSIE_SQL_METRIC = (OSIDialect.OSSIE_SQL_2026, "SUM(orders.amount)")
_ANSI_METRIC = (OSIDialect.ANSI_SQL, "SUM(orders.amount_ansi)")


def _doc_with_metric_expression(expression: OSIExpression) -> OSIDocument:
    return _osi_doc(
        datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
        metrics=[OSIMetric(name="revenue", expression=expression)],
    )


class TestOSIToMSIDialectSelection:  # noqa: D101
    @pytest.mark.parametrize(
        "order",
        [
            (_OSSIE_SQL_METRIC, _SNOWFLAKE_METRIC),
            (_SNOWFLAKE_METRIC, _OSSIE_SQL_METRIC),
        ],
        ids=["osi_sql_first", "vendor_first"],
    )
    def test_osi_sql_2026_wins_over_a_vendor_dialect_in_either_order(  # noqa: D102
        self, order: tuple[tuple[OSIDialect, str], ...]
    ) -> None:
        doc = _doc_with_metric_expression(_multi_dialect_expr(*order))

        result = OSIToMSIConverter().convert(doc).output

        assert result.metrics[0].type_params.expr == "amount"

    @pytest.mark.parametrize(
        "order",
        [
            (_ANSI_METRIC, _OSSIE_SQL_METRIC),
            (_OSSIE_SQL_METRIC, _ANSI_METRIC),
        ],
        ids=["ansi_first", "osi_sql_first"],
    )
    def test_ansi_sql_still_takes_precedence_over_osi_sql_2026(  # noqa: D102
        self, order: tuple[tuple[OSIDialect, str], ...]
    ) -> None:
        doc = _doc_with_metric_expression(_multi_dialect_expr(*order))

        result = OSIToMSIConverter().convert(doc).output

        assert result.metrics[0].type_params.expr == "amount_ansi"

    def test_field_expression_prefers_osi_sql_2026_over_a_vendor_dialect(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset(
                    "orders",
                    fields=[
                        OSIField(
                            name="region",
                            expression=_multi_dialect_expr(
                                (OSIDialect.SNOWFLAKE, "region_snowflake_only"),
                                (OSIDialect.OSSIE_SQL_2026, "region_portable"),
                            ),
                        )
                    ],
                )
            ]
        )

        sm = OSIToMSIConverter().convert(doc).output.semantic_models[0]

        assert [(d.name, d.expr) for d in sm.dimensions] == [("region", "region_portable")]

    def test_first_entry_is_still_used_when_no_portable_dialect_is_present(self) -> None:  # noqa: D102
        doc = _doc_with_metric_expression(
            _multi_dialect_expr(
                (OSIDialect.SNOWFLAKE, "SUM(orders.amt_snowflake_only)"),
                (OSIDialect.DAX, "SUM(orders.amt_dax_only)"),
            )
        )

        result = OSIToMSIConverter().convert(doc).output

        assert result.metrics[0].type_params.expr == "amt_snowflake_only"

    def test_the_first_osi_sql_2026_entry_wins_when_the_dialect_is_repeated(self) -> None:  # noqa: D102
        # `dialects` has no `uniqueItems` constraint, so the same dialect may be listed twice.
        doc = _doc_with_metric_expression(
            _multi_dialect_expr(
                (OSIDialect.OSSIE_SQL_2026, "SUM(orders.amount)"),
                (OSIDialect.OSSIE_SQL_2026, "SUM(orders.amount_duplicate)"),
            )
        )

        result = OSIToMSIConverter().convert(doc).output

        assert result.metrics[0].type_params.expr == "amount"

    def test_the_dataset_of_a_column_is_resolved_with_the_same_dialect_preference(self) -> None:  # noqa: D102
        # `unrelated` is declared first, so resolving the field expression positionally
        # would miss `orders` and fall back to it.
        doc = _osi_doc(
            datasets=[
                _osi_dataset("unrelated", fields=[_osi_field("other_column")]),
                _osi_dataset(
                    "orders",
                    fields=[
                        OSIField(
                            name="amount",
                            expression=_multi_dialect_expr(
                                (OSIDialect.SNOWFLAKE, "amt_snowflake_only"),
                                (OSIDialect.OSSIE_SQL_2026, "amount_portable"),
                            ),
                        )
                    ],
                ),
            ],
            metrics=[
                OSIMetric(
                    name="revenue",
                    expression=_multi_dialect_expr(
                        (OSIDialect.OSSIE_SQL_2026, "SUM(amount_portable)"),
                    ),
                )
            ],
        )

        result = OSIToMSIConverter().convert(doc).output

        agg_params = result.metrics[0].type_params.metric_aggregation_params
        assert agg_params is not None
        assert agg_params.semantic_model == "orders"


class TestOSIToMSIReviewRegressions:  # noqa: D101
    @pytest.mark.parametrize(
        ("expression", "agg", "scalar"),
        [
            ("SUM(price * qty)", AggregationType.SUM, "price * qty"),
            (
                "SUM(CASE WHEN status='paid' THEN 1 ELSE 0 END)",
                AggregationType.SUM_BOOLEAN,
                "status = 'paid'",
            ),
            (
                "SUM(CASE WHEN status='paid' THEN amount ELSE 0 END)",
                AggregationType.SUM,
                "CASE WHEN status = 'paid' THEN amount ELSE 0 END",
            ),
            (
                "SUM(CASE status WHEN 'paid' THEN 1 ELSE 0 END)",
                AggregationType.SUM,
                "CASE status WHEN 'paid' THEN 1 ELSE 0 END",
            ),
            ("AVG(POWER(price, 2))", AggregationType.AVERAGE, "POWER(price, 2)"),
            ("SUM(orders.price * qty)", AggregationType.SUM, "price * qty"),
            ("SUM(orders.price * orders.qty)", AggregationType.SUM, "price * qty"),
        ],
    )
    def test_review_compound_columns_resolve_individually(  # noqa: D102
        self, expression: str, agg: AggregationType, scalar: str
    ) -> None:
        doc = _osi_doc(
            datasets=[
                _osi_dataset("refunds", fields=[_osi_field("refund_id")]),
                _osi_dataset(
                    "orders",
                    fields=[_osi_field(name) for name in ("price", "qty", "status", "amount")],
                ),
            ],
            metrics=[_osi_metric("m", expression)],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.issues == []
        metric = result.output.metrics[0]
        assert metric.type_params.expr == scalar
        assert _agg_params(metric).agg == agg
        assert _agg_params(metric).semantic_model == "orders"

    @pytest.mark.parametrize(
        "expression",
        [
            "SUM(price * fee)",
            "SUM(price * missing)",
            "SUM(amount * price)",
            "SUM(orders.price + amount)",
            "SUM(nope.price)",
            "SUM(CASE WHEN refunds.fee > 0 THEN orders.price ELSE 0 END)",
        ],
    )
    def test_review_ambiguous_or_cross_model_columns_are_still_refused(self, expression: str) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset("refunds", fields=[_osi_field("fee"), _osi_field("amount")]),
                _osi_dataset("orders", fields=[_osi_field("price"), _osi_field("amount")]),
            ],
            metrics=[_osi_metric("m", expression)],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert result.issues == [ConverterIssue(ConverterIssueType.UNSUPPORTED_METRIC_EXPRESSION, "m")]

    @pytest.mark.parametrize("expression", ["SUM(o.amount)", "SUM(ORDERS.amount)"])
    def test_review_single_dataset_alias_is_bound_and_unqualified(self, expression: str) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("m", expression)],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.issues == []
        assert result.output.metrics[0].type_params.expr == "amount"
        assert _agg_params(result.output.metrics[0]).semantic_model == "orders"

    def test_review_multiple_unknown_aliases_do_not_bind_to_the_only_dataset(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("m", "SUM(o.amount + r.amount)")],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert result.issues == [ConverterIssue(ConverterIssueType.UNSUPPORTED_METRIC_EXPRESSION, "m")]

    def test_review_quoted_identifiers_remain_quoted_in_scalar_output(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("Amount Paid")])],
            metrics=[_osi_metric("m", 'SUM(orders."Amount Paid")')],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.issues == []
        assert result.output.metrics[0].type_params.expr == '"Amount Paid"'

    def test_review_casefold_qualifier_does_not_select_between_duplicate_dataset_leaves(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset("a.orders", fields=[_osi_field("amount")]),
                _osi_dataset("b.orders", fields=[_osi_field("amount")]),
            ],
            metrics=[_osi_metric("m", "SUM(ORDERS.amount)")],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert result.issues == [ConverterIssue(ConverterIssueType.UNSUPPORTED_METRIC_EXPRESSION, "m")]

    def test_review_compound_physical_columns_resolve_through_field_expressions(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset("refunds", fields=[_osi_field("id")]),
                _osi_dataset(
                    "orders",
                    fields=[_osi_field("price", "unit_price"), _osi_field("qty", "unit_count")],
                ),
            ],
            metrics=[_osi_metric("m", "SUM(unit_price * unit_count)")],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.issues == []
        assert result.output.metrics[0].type_params.expr == "unit_price * unit_count"
        assert _agg_params(result.output.metrics[0]).semantic_model == "orders"

    def test_review_boolean_scalar_loses_all_qualifiers(self) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("status")])],
            metrics=[_osi_metric("m", "SUM(CASE WHEN orders.status = 'paid' THEN 1 ELSE 0 END)")],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.issues == []
        assert result.output.metrics[0].type_params.expr == "status = 'paid'"

    @pytest.mark.parametrize(
        "scalar",
        [
            "DATEDIFF(day, start_date, end_date)",
            "DATEADD(day, 1, order_date)",
            "DATE_PART('year', order_date)",
            "YEAR(order_date)",
            "QUARTER(order_date)",
            "MONTH(order_date)",
            "DAY(order_date)",
            "DAYOFYEAR(order_date)",
            "HOUR(order_date)",
            "MINUTE(order_date)",
            "SECOND(order_date)",
            "DATE_TRUNC('month', order_date)",
            "order_date + INTERVAL '1' DAY",
            "CONCAT(status, region)",
            "LEFT(status, 2)",
            "RIGHT(status, 2)",
            "LTRIM(status)",
            "RTRIM(status)",
            "REPLACE(status, 'a', 'b')",
            "SPLIT_PART(status, '-', 1)",
            "POSITION('a' IN status)",
            "CHARINDEX('a', status)",
            "CONTAINS(status, 'a')",
            "STARTSWITH(status, 'a')",
            "ENDSWITH(status, 'a')",
            "REGEXP_LIKE(status, 'a')",
            "TRUNC(amount, 2)",
            "SIGN(amount)",
            "POWER(amount, 2)",
            "SQRT(amount)",
            "EXP(amount)",
            "LN(amount)",
            "LOG(2, amount)",
            "LOG10(amount)",
            "NVL2(amount, amount, 0)",
            "ZEROIFNULL(amount)",
            "NULLIFZERO(amount)",
            "TRY_CAST(amount AS DECIMAL(10, 2))",
            "CASE WHEN (status, region) IN (('paid', 'west')) THEN amount ELSE 0 END",
        ],
    )
    def test_review_required_scalar_functions_preserve_one_aggregate(self, scalar: str) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[
                _osi_dataset("unrelated", fields=[_osi_field("id")]),
                _osi_dataset(
                    "orders",
                    fields=[
                        _osi_field(name)
                        for name in ("start_date", "end_date", "order_date", "status", "region", "amount")
                    ],
                ),
            ],
            metrics=[_osi_metric("m", f"MAX({scalar})", dialect=OSIDialect.OSSIE_SQL_2026)],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.issues == []
        metric = result.output.metrics[0]
        assert _agg_params(metric).agg == AggregationType.MAX
        assert _agg_params(metric).semantic_model == "orders"
        assert metric.type_params.expr == sqlglot.parse_one(scalar, read="snowflake").sql(dialect="snowflake")

    @pytest.mark.parametrize(
        "expression",
        [
            "SUM(POWER(MY_UDAF(amount), 2))",
            "SUM(CONCAT(status, (SELECT region FROM other_table)))",
            "MAX(SUM(amount) OVER ())",
            "SUM(DISTINCT POWER(amount, 2))",
        ],
    )
    def test_review_new_scalar_support_does_not_admit_unsafe_children(self, expression: str) -> None:  # noqa: D102
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("amount")])],
            metrics=[_osi_metric("m", expression)],
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.output.metrics == []
        assert result.issues == [ConverterIssue(ConverterIssueType.UNSUPPORTED_METRIC_EXPRESSION, "m")]

    @pytest.mark.parametrize("preferred", [OSIDialect.ANSI_SQL, OSIDialect.DAX])
    def test_review_non_sql_preference_falls_back_to_portable_metric(self, preferred: OSIDialect) -> None:  # noqa: D102
        doc = _doc_with_metric_expression(
            _multi_dialect_expr(
                (OSIDialect.DAX, "SUM(Sales[Amount])"),
                _SNOWFLAKE_METRIC,
                _OSSIE_SQL_METRIC,
            )
        )
        result = OSIToMSIConverter(dialect=preferred).convert(doc)

        assert result.issues == []
        assert result.output.metrics[0].type_params.expr == "amount"

    def test_review_non_sql_first_entry_falls_back_to_first_readable_sql(self) -> None:  # noqa: D102
        doc = _doc_with_metric_expression(
            _multi_dialect_expr((OSIDialect.DAX, "SUM(Sales[Amount])"), _SNOWFLAKE_METRIC)
        )
        result = OSIToMSIConverter().convert(doc)

        assert result.issues == []
        assert result.output.metrics[0].type_params.expr == "amt_snowflake_only"

    def test_review_configured_sql_dialect_still_wins_over_portable_alternative(self) -> None:  # noqa: D102
        doc = _doc_with_metric_expression(_multi_dialect_expr(_OSSIE_SQL_METRIC, _SNOWFLAKE_METRIC))
        result = OSIToMSIConverter(dialect=OSIDialect.SNOWFLAKE).convert(doc)

        assert result.issues == []
        assert result.output.metrics[0].type_params.expr == "amt_snowflake_only"

    @pytest.mark.parametrize(
        "dialect",
        [
            OSIDialect.DAX,
            OSIDialect.MDX,
            OSIDialect.MAQL,
            OSIDialect.TABLEAU,
            OSIDialect.SIGMA,
            OSIDialect.THOUGHTSPOT,
        ],
    )
    def test_review_non_sql_only_is_refused_even_if_it_looks_like_sql(self, dialect: OSIDialect) -> None:  # noqa: D102
        doc = _doc_with_metric_expression(_multi_dialect_expr((dialect, "SUM(orders.amount)")))
        result = OSIToMSIConverter(dialect=dialect).convert(doc)

        assert result.output.metrics == []
        assert result.issues == [ConverterIssue(ConverterIssueType.UNSUPPORTED_METRIC_EXPRESSION, "revenue")]

    @pytest.mark.parametrize(
        ("dialect", "scalar"),
        [
            (OSIDialect.SNOWFLAKE, "DATEDIFF(day, orders.start_date, orders.end_date)"),
            (OSIDialect.DATABRICKS, "DATEADD(day, 1, orders.start_date)"),
            (OSIDialect.BIGQUERY, "DATE_DIFF(orders.end_date, orders.start_date, DAY)"),
        ],
    )
    def test_review_selected_sql_dialect_is_used_for_parse_and_render(  # noqa: D102
        self, dialect: OSIDialect, scalar: str
    ) -> None:
        doc = _osi_doc(
            datasets=[_osi_dataset("orders", fields=[_osi_field("start_date"), _osi_field("end_date")])],
            metrics=[_osi_metric("m", f"MAX({scalar}) / MAX({scalar})", dialect=dialect)],
        )
        result = OSIToMSIConverter(dialect=dialect).convert(doc)

        assert result.issues == []
        assert len(result.output.metrics) == 3
        for metric in result.output.metrics[:2]:
            assert _agg_params(metric).semantic_model == "orders"
            assert metric.type_params.expr is not None
            assert "orders." not in metric.type_params.expr
            tree = sqlglot.parse_one(metric.type_params.expr, read=dialect.value.lower())
            assert {column.name for column in tree.find_all(sqlglot.exp.Column)} <= {"start_date", "end_date"}
