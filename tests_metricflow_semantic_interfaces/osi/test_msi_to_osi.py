from __future__ import annotations

import json

from metricflow.converters.osi.filter_utils import _render_filter_template
from metricflow.converters.osi.models import OSIDialect
from metricflow.converters.osi.msi_to_osi import MSIToOSIConverter
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticConversionTypeParams,
    PydanticCumulativeTypeParams,
    PydanticMetric,
    PydanticMetricAggregationParams,
    PydanticMetricInput,
    PydanticMetricInputMeasure,
    PydanticMetricTimeWindow,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.implementations.semantic_model import (
    PydanticNodeRelation,
    PydanticSemanticModel,
)
from metricflow_semantic_interfaces.test_utils import default_meta, semantic_model_with_guaranteed_meta
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
    _filter,
    _manifest,
    _measure,
    _simple_metric,
)


class TestBasicConversion:  # noqa: D101
    def test_empty_manifest_produces_empty_datasets(self) -> None:  # noqa: D102
        result = MSIToOSIConverter().convert(_manifest(), model_name="test")

        assert result.version == "0.1.1"
        assert len(result.semantic_model) == 1
        assert result.semantic_model[0].name == "test"
        assert result.semantic_model[0].datasets == []
        assert result.semantic_model[0].metrics is None
        assert result.semantic_model[0].relationships is None

    def test_semantic_model_becomes_dataset(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            description="Order data",
            node_relation=PydanticNodeRelation(schema_name="analytics", alias="orders_table"),
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        dataset = result.semantic_model[0].datasets[0]
        assert dataset.name == "orders"
        assert dataset.source == "analytics.orders_table"
        assert dataset.description == "Order data"

    def test_source_includes_database_when_present(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            node_relation=PydanticNodeRelation(schema_name="analytics", alias="orders_table", database="prod"),
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        assert result.semantic_model[0].datasets[0].source == "prod.analytics.orders_table"

    def test_multiple_semantic_models_become_multiple_datasets(self) -> None:  # noqa: D102
        sm_a = semantic_model_with_guaranteed_meta(name="orders")
        sm_b = semantic_model_with_guaranteed_meta(name="users")
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm_a, sm_b]))

        names = [ds.name for ds in result.semantic_model[0].datasets]
        assert names == ["orders", "users"]


class TestDimensionConversion:  # noqa: D101
    def test_categorical_dimension_has_is_time_false(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("status", dim_type=DimensionType.CATEGORICAL)],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        field = fields[0]
        assert field.name == "status"
        assert field.dimension is not None
        assert field.dimension.is_time is False

    def test_time_dimension_has_is_time_true(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("ds", dim_type=DimensionType.TIME, granularity=TimeGranularity.DAY)],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        field = fields[0]
        assert field.dimension is not None
        assert field.dimension.is_time is True

    def test_dimension_with_expr_uses_expr(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("order_date", expr="DATE(created_at)")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].expression.dialects[0].expression == "DATE(created_at)"

    def test_dimension_without_expr_falls_back_to_name(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("status")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].expression.dialects[0].expression == "status"

    def test_dimension_description_and_label_carried_over(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("status", description="Order status", label="Status")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].description == "Order status"
        assert fields[0].label == "Status"


class TestMeasureConversion:  # noqa: D101
    def test_measure_becomes_field_without_dimension_metadata(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("revenue", agg=AggregationType.SUM, expr="amount")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        field = fields[0]
        assert field.name == "revenue"
        assert field.expression.dialects[0].expression == "amount"
        assert field.dimension is None

    def test_measure_without_expr_falls_back_to_name(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("num_orders", agg=AggregationType.COUNT)],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].expression.dialects[0].expression == "num_orders"

    def test_measure_description_and_label_carried_over(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("revenue", description="Total revenue", label="Revenue")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].description == "Total revenue"
        assert fields[0].label == "Revenue"


class TestEntityConversion:  # noqa: D101
    def test_entity_becomes_field(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("order_id", entity_type=EntityType.PRIMARY)],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        field = fields[0]
        assert field.name == "order_id"
        assert field.expression.dialects[0].expression == "order_id"
        assert field.dimension is None

    def test_entity_with_expr_uses_expr(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("order_id", entity_type=EntityType.PRIMARY, expr="id")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        field = fields[0]
        assert field.name == "order_id"
        assert field.expression.dialects[0].expression == "id"

    def test_foreign_entity_also_becomes_field(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("user_id", entity_type=EntityType.FOREIGN)],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].name == "user_id"


class TestEntityKeyExtraction:  # noqa: D101
    def test_primary_entity_becomes_primary_key(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("order_id", entity_type=EntityType.PRIMARY)],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        assert result.semantic_model[0].datasets[0].primary_key == ["order_id"]

    def test_primary_entity_with_expr_uses_expr_in_key(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("order_id", entity_type=EntityType.PRIMARY, expr="id")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        assert result.semantic_model[0].datasets[0].primary_key == ["id"]

    def test_unique_entity_becomes_unique_key(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("email", entity_type=EntityType.UNIQUE)],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        assert result.semantic_model[0].datasets[0].unique_keys == [["email"]]

    def test_foreign_entity_does_not_appear_in_keys(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("user_id", entity_type=EntityType.FOREIGN)],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        dataset = result.semantic_model[0].datasets[0]
        assert dataset.primary_key is None
        assert dataset.unique_keys is None


class TestFieldOrdering:  # noqa: D101
    def test_entities_then_dimensions_then_measures(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("order_id", entity_type=EntityType.PRIMARY)],
            dimensions=[_dimension("status")],
            measures=[_measure("revenue", expr="amount")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].name == "order_id"
        assert fields[1].name == "status"
        assert fields[2].name == "revenue"


class TestDialectConfiguration:  # noqa: D101
    def test_default_dialect_is_ansi_sql(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("status")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        assert result.dialects == [OSIDialect.ANSI_SQL]
        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].expression.dialects[0].dialect == OSIDialect.ANSI_SQL

    def test_configurable_dialect(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            dimensions=[_dimension("status")],
        )
        result = MSIToOSIConverter(dialect=OSIDialect.SNOWFLAKE).convert(_manifest(semantic_models=[sm]))

        assert result.dialects == [OSIDialect.SNOWFLAKE]
        fields = result.semantic_model[0].datasets[0].fields
        assert fields is not None
        assert fields[0].expression.dialects[0].dialect == OSIDialect.SNOWFLAKE


class TestRelationshipConversion:  # noqa: D101
    def test_shared_entity_name_produces_relationship(self) -> None:  # noqa: D102
        listings = semantic_model_with_guaranteed_meta(
            name="listings",
            entities=[_entity("listing", entity_type=EntityType.PRIMARY, expr="listing_id")],
        )
        bookings = semantic_model_with_guaranteed_meta(
            name="bookings",
            entities=[_entity("listing", entity_type=EntityType.FOREIGN, expr="listing_id")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[listings, bookings]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        assert len(rels) == 1
        rel = rels[0]
        assert rel.from_dataset == "bookings"
        assert rel.to == "listings"
        assert rel.from_columns == ["listing_id"]
        assert rel.to_columns == ["listing_id"]

    def test_same_type_entities_produce_relationship(self) -> None:  # noqa: D102
        users_a = semantic_model_with_guaranteed_meta(
            name="users_a",
            entities=[_entity("user", entity_type=EntityType.PRIMARY, expr="user_id")],
        )
        users_b = semantic_model_with_guaranteed_meta(
            name="users_b",
            entities=[_entity("user", entity_type=EntityType.PRIMARY, expr="uid")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[users_a, users_b]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        assert len(rels) == 1
        assert rels[0].from_columns == ["user_id"]
        assert rels[0].to_columns == ["uid"]

    def test_single_dataset_with_entity_produces_no_relationship(self) -> None:  # noqa: D102
        bookings = semantic_model_with_guaranteed_meta(
            name="bookings",
            entities=[_entity("listing", entity_type=EntityType.FOREIGN, expr="listing_id")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[bookings]))

        assert result.semantic_model[0].relationships is None

    def test_same_dataset_entities_excluded(self) -> None:  # noqa: D102
        orders = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[
                _entity("order", entity_type=EntityType.PRIMARY, expr="order_id"),
                _entity("order", entity_type=EntityType.FOREIGN, expr="order_id"),
            ],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[orders]))

        assert result.semantic_model[0].relationships is None

    def test_three_datasets_produce_all_pairs(self) -> None:  # noqa: D102
        users_a = semantic_model_with_guaranteed_meta(
            name="users_a",
            entities=[_entity("user", entity_type=EntityType.PRIMARY, expr="user_id")],
        )
        users_b = semantic_model_with_guaranteed_meta(
            name="users_b",
            entities=[_entity("user", entity_type=EntityType.UNIQUE, expr="user_id")],
        )
        orders = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("user", entity_type=EntityType.FOREIGN, expr="user_id")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[users_a, users_b, orders]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        assert len(rels) == 3
        pairs = {(r.from_dataset, r.to) for r in rels}
        assert pairs == {("users_a", "users_b"), ("orders", "users_a"), ("orders", "users_b")}

    def test_columns_use_expr_when_present(self) -> None:  # noqa: D102
        listings = semantic_model_with_guaranteed_meta(
            name="listings",
            entities=[_entity("listing", entity_type=EntityType.PRIMARY, expr="lid")],
        )
        bookings = semantic_model_with_guaranteed_meta(
            name="bookings",
            entities=[_entity("listing", entity_type=EntityType.FOREIGN, expr="fk_lid")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[listings, bookings]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        rel = rels[0]
        assert rel.from_columns == ["fk_lid"]
        assert rel.to_columns == ["lid"]

    def test_columns_fall_back_to_name_without_expr(self) -> None:  # noqa: D102
        listings = semantic_model_with_guaranteed_meta(
            name="listings",
            entities=[_entity("listing", entity_type=EntityType.PRIMARY)],
        )
        bookings = semantic_model_with_guaranteed_meta(
            name="bookings",
            entities=[_entity("listing", entity_type=EntityType.FOREIGN)],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[listings, bookings]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        assert rels[0].from_columns == ["listing"]
        assert rels[0].to_columns == ["listing"]

    def test_primary_entity_shorthand_does_not_produce_relationship(self) -> None:  # noqa: D102
        bookings = PydanticSemanticModel(
            name="bookings",
            description=None,
            node_relation=PydanticNodeRelation(schema_name="schema", alias="table"),
            primary_entity="booking",
            entities=[],
            metadata=default_meta(),
        )
        orders = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("booking", entity_type=EntityType.FOREIGN, expr="booking_id")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[bookings, orders]))

        assert result.semantic_model[0].relationships is None

    def test_relationship_name_format(self) -> None:  # noqa: D102
        listings = semantic_model_with_guaranteed_meta(
            name="listings",
            entities=[_entity("listing", entity_type=EntityType.PRIMARY, expr="listing_id")],
        )
        bookings = semantic_model_with_guaranteed_meta(
            name="bookings",
            entities=[_entity("listing", entity_type=EntityType.FOREIGN, expr="listing_id")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[listings, bookings]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        assert rels[0].name == "bookings__listings__listing"

    def test_natural_entity_excluded(self) -> None:  # noqa: D102
        users = semantic_model_with_guaranteed_meta(
            name="users",
            entities=[_entity("user", entity_type=EntityType.NATURAL, expr="user_id")],
        )
        orders = semantic_model_with_guaranteed_meta(
            name="orders",
            entities=[_entity("user", entity_type=EntityType.FOREIGN, expr="user_id")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[users, orders]))

        assert result.semantic_model[0].relationships is None

    def test_direction_based_on_entity_type_not_manifest_order(self) -> None:  # noqa: D102
        beta = semantic_model_with_guaranteed_meta(
            name="beta",
            entities=[_entity("shared", entity_type=EntityType.PRIMARY, expr="col_b")],
        )
        alpha = semantic_model_with_guaranteed_meta(
            name="alpha",
            entities=[_entity("shared", entity_type=EntityType.FOREIGN, expr="col_a")],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[beta, alpha]))

        rels = result.semantic_model[0].relationships
        assert rels is not None
        assert rels[0].from_dataset == "alpha"
        assert rels[0].to == "beta"


class TestMetricConversion:  # noqa: D101
    # --- SIMPLE ---

    def test_simple_metric_resolves_through_measure(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("revenue", agg=AggregationType.SUM, expr="amount")],
        )
        metric = _simple_metric("revenue", measure_name="revenue")
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[metric]))

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        assert len(metrics) == 1
        assert metrics[0].name == "revenue"
        assert metrics[0].expression.dialects[0].expression == "SUM(amount)"

    def test_simple_metric_description_carried_over(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("revenue", agg=AggregationType.SUM, expr="amount")],
        )
        metric = _simple_metric("revenue", measure_name="revenue", description="Total revenue")
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[metric]))

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        assert metrics[0].description == "Total revenue"

    def test_simple_metric_with_metric_aggregation_params(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(name="orders")
        metric = PydanticMetric(
            name="avg_price",
            description=None,
            type=MetricType.SIMPLE,
            type_params=PydanticMetricTypeParams(
                expr="price",
                metric_aggregation_params=PydanticMetricAggregationParams(
                    semantic_model="orders",
                    agg=AggregationType.AVERAGE,
                    agg_params=None,
                    agg_time_dimension=None,
                    non_additive_dimension=None,
                ),
            ),
            filter=None,
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[metric]))

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        assert metrics[0].expression.dialects[0].expression == "AVG(price)"

    # --- RATIO ---

    def test_ratio_metric_inlines_sub_expressions(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[
                _measure("revenue", agg=AggregationType.SUM, expr="amount"),
                _measure("order_count", agg=AggregationType.COUNT, expr="order_id"),
            ],
        )
        revenue_m = _simple_metric("revenue", "revenue")
        order_count_m = _simple_metric("order_count", "order_count")
        arpu = PydanticMetric(
            name="arpu",
            description=None,
            type=MetricType.RATIO,
            type_params=PydanticMetricTypeParams(
                numerator=PydanticMetricInput(name="revenue"),
                denominator=PydanticMetricInput(name="order_count"),
            ),
            filter=None,
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[revenue_m, order_count_m, arpu]))

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        arpu_osi = next(m for m in metrics if m.name == "arpu")
        assert arpu_osi.expression.dialects[0].expression == "(SUM(amount)) / (COUNT(order_id))"

    # --- DERIVED ---

    def test_derived_metric_inlines_sub_expressions(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[
                _measure("revenue", agg=AggregationType.SUM, expr="amount"),
                _measure("cost", agg=AggregationType.SUM, expr="cost_amount"),
            ],
        )
        revenue_m = _simple_metric("revenue", "revenue")
        cost_m = _simple_metric("cost", "cost")
        profit = PydanticMetric(
            name="profit",
            description=None,
            type=MetricType.DERIVED,
            type_params=PydanticMetricTypeParams(
                expr="revenue - cost",
                metrics=[
                    PydanticMetricInput(name="revenue"),
                    PydanticMetricInput(name="cost"),
                ],
            ),
            filter=None,
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[revenue_m, cost_m, profit]))

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        profit_osi = next(m for m in metrics if m.name == "profit")
        assert profit_osi.expression.dialects[0].expression == "SUM(amount) - SUM(cost_amount)"

    def test_derived_metric_uses_alias_for_substitution(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[
                _measure("revenue", agg=AggregationType.SUM, expr="amount"),
                _measure("cost", agg=AggregationType.SUM, expr="cost_amount"),
            ],
        )
        revenue_m = _simple_metric("revenue", "revenue")
        cost_m = _simple_metric("cost", "cost")
        profit = PydanticMetric(
            name="profit",
            description=None,
            type=MetricType.DERIVED,
            type_params=PydanticMetricTypeParams(
                expr="r - c",
                metrics=[
                    PydanticMetricInput(name="revenue", alias="r"),
                    PydanticMetricInput(name="cost", alias="c"),
                ],
            ),
            filter=None,
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[revenue_m, cost_m, profit]))

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        profit_osi = next(m for m in metrics if m.name == "profit")
        assert profit_osi.expression.dialects[0].expression == "SUM(amount) - SUM(cost_amount)"

    def test_derived_metric_nested(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[
                _measure("revenue", agg=AggregationType.SUM, expr="amount"),
                _measure("cost", agg=AggregationType.SUM, expr="cost_amount"),
                _measure("expenses", agg=AggregationType.SUM, expr="expense_amount"),
            ],
        )
        revenue_m = _simple_metric("revenue", "revenue")
        cost_m = _simple_metric("cost", "cost")
        expenses_m = _simple_metric("expenses", "expenses")
        gross_profit = PydanticMetric(
            name="gross_profit",
            description=None,
            type=MetricType.DERIVED,
            type_params=PydanticMetricTypeParams(
                expr="revenue - cost",
                metrics=[
                    PydanticMetricInput(name="revenue"),
                    PydanticMetricInput(name="cost"),
                ],
            ),
            filter=None,
            metadata=default_meta(),
            config=None,
        )
        net_profit = PydanticMetric(
            name="net_profit",
            description=None,
            type=MetricType.DERIVED,
            type_params=PydanticMetricTypeParams(
                expr="gross_profit - expenses",
                metrics=[
                    PydanticMetricInput(name="gross_profit"),
                    PydanticMetricInput(name="expenses"),
                ],
            ),
            filter=None,
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(
            _manifest(
                semantic_models=[sm],
                metrics=[revenue_m, cost_m, expenses_m, gross_profit, net_profit],
            )
        )

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        net_osi = next(m for m in metrics if m.name == "net_profit")
        assert net_osi.expression.dialects[0].expression == "(SUM(amount) - SUM(cost_amount)) - SUM(expense_amount)"

    def test_derived_metric_ref_not_corrupted_by_prefix_match(self) -> None:  # noqa: D102
        """Substituting 'revenue' must not corrupt 'revenue_adjusted' when it appears in the same expr."""
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[
                _measure("revenue", agg=AggregationType.SUM, expr="amount"),
                _measure("revenue_adjusted", agg=AggregationType.SUM, expr="adjusted_amount"),
            ],
        )
        revenue_m = _simple_metric("revenue", "revenue")
        revenue_adjusted_m = _simple_metric("revenue_adjusted", "revenue_adjusted")
        derived = PydanticMetric(
            name="revenue_delta",
            description=None,
            type=MetricType.DERIVED,
            type_params=PydanticMetricTypeParams(
                expr="revenue - revenue_adjusted",
                metrics=[
                    PydanticMetricInput(name="revenue"),
                    PydanticMetricInput(name="revenue_adjusted"),
                ],
            ),
            filter=None,
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(
            _manifest(semantic_models=[sm], metrics=[revenue_m, revenue_adjusted_m, derived])
        )

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        derived_osi = next(m for m in metrics if m.name == "revenue_delta")
        assert derived_osi.expression.dialects[0].expression == "SUM(amount) - SUM(adjusted_amount)"

    # --- CUMULATIVE ---

    def test_cumulative_metric_uses_base_aggregation(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("revenue", agg=AggregationType.SUM, expr="amount")],
        )
        cumulative = PydanticMetric(
            name="cumulative_revenue",
            description=None,
            type=MetricType.CUMULATIVE,
            type_params=PydanticMetricTypeParams(
                measure=PydanticMetricInputMeasure(name="revenue"),
                cumulative_type_params=PydanticCumulativeTypeParams(
                    window=PydanticMetricTimeWindow(count=7, granularity="day"),
                ),
            ),
            filter=None,
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[cumulative]))

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        assert len(metrics) == 1
        assert metrics[0].name == "cumulative_revenue"
        assert metrics[0].expression.dialects[0].expression == "SUM(amount)"

    def test_cumulative_metric_via_sub_metric_reference(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("revenue", agg=AggregationType.SUM, expr="amount")],
        )
        base = PydanticMetric(
            name="total_revenue",
            description=None,
            type=MetricType.SIMPLE,
            type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name="revenue")),
            filter=None,
            metadata=default_meta(),
            config=None,
        )
        cumulative = PydanticMetric(
            name="cumulative_revenue",
            description=None,
            type=MetricType.CUMULATIVE,
            type_params=PydanticMetricTypeParams(
                cumulative_type_params=PydanticCumulativeTypeParams(
                    metric=PydanticMetricInput(name="total_revenue"),
                ),
            ),
            filter=None,
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[base, cumulative]))

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        cumulative_osi = next(m for m in metrics if m.name == "cumulative_revenue")
        assert cumulative_osi.expression.dialects[0].expression == "SUM(amount)"

    # --- Edge cases ---

    def test_no_metrics_produces_no_osi_metrics(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(name="orders")
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))

        assert result.semantic_model[0].metrics is None

    def test_multiple_metrics_all_converted(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[
                _measure("revenue", agg=AggregationType.SUM, expr="amount"),
                _measure("order_count", agg=AggregationType.COUNT, expr="order_id"),
            ],
        )
        result = MSIToOSIConverter().convert(
            _manifest(
                semantic_models=[sm],
                metrics=[
                    _simple_metric("revenue", "revenue"),
                    _simple_metric("order_count", "order_count"),
                ],
            )
        )

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        assert len(metrics) == 2
        names = {m.name for m in metrics}
        assert names == {"revenue", "order_count"}

    def test_conversion_metric_skipped(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[
                _measure("visits", agg=AggregationType.COUNT, expr="visit_id"),
                _measure("purchases", agg=AggregationType.COUNT, expr="purchase_id"),
            ],
        )
        conversion = PydanticMetric(
            name="purchase_rate",
            description=None,
            type=MetricType.CONVERSION,
            type_params=PydanticMetricTypeParams(
                conversion_type_params=PydanticConversionTypeParams(
                    base_measure=PydanticMetricInputMeasure(name="visits"),
                    conversion_measure=PydanticMetricInputMeasure(name="purchases"),
                    entity="user",
                ),
            ),
            filter=None,
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[conversion]))

        assert result.semantic_model[0].metrics is None


class TestFilterRendering:  # noqa: D101
    """Unit tests for the Jinja → SQL rendering of where-filter templates."""

    def test_plain_sql_passthrough(self) -> None:  # noqa: D102
        assert _render_filter_template("status = 'paid'") == "status = 'paid'"

    def test_dimension_reference(self) -> None:  # noqa: D102
        assert _render_filter_template("{{ Dimension('order__status') }} = 'paid'") == "order__status = 'paid'"

    def test_dimension_with_grain(self) -> None:  # noqa: D102
        result = _render_filter_template("{{ Dimension('order__ds').grain('day') }} >= '2023-01-01'")
        assert result == "order__ds__day >= '2023-01-01'"

    def test_time_dimension_with_grain_arg(self) -> None:  # noqa: D102
        result = _render_filter_template("{{ TimeDimension('order__ds', 'week') }} >= '2023-01-01'")
        assert result == "order__ds__week >= '2023-01-01'"

    def test_time_dimension_without_grain(self) -> None:  # noqa: D102
        assert _render_filter_template("{{ TimeDimension('metric_time') }} IS NOT NULL") == "metric_time IS NOT NULL"

    def test_entity_reference(self) -> None:  # noqa: D102
        assert _render_filter_template("{{ Entity('user') }} != 'bot'") == "user != 'bot'"

    def test_metric_reference(self) -> None:  # noqa: D102
        assert _render_filter_template("{{ Metric('revenue') }} > 0") == "revenue > 0"


class TestMetricFilterFlattening:  # noqa: D101
    def test_metric_level_filter_inlines_case_when(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("revenue", agg=AggregationType.SUM, expr="amount")],
        )
        metric = PydanticMetric(
            name="paid_revenue",
            description=None,
            type=MetricType.SIMPLE,
            type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name="revenue")),
            filter=_filter("status = 'paid'"),
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[metric]))

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        assert metrics[0].expression.dialects[0].expression == "SUM(CASE WHEN status = 'paid' THEN amount END)"

    def test_measure_level_filter_inlines_case_when(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("revenue", agg=AggregationType.SUM, expr="amount")],
        )
        metric = PydanticMetric(
            name="paid_revenue",
            description=None,
            type=MetricType.SIMPLE,
            type_params=PydanticMetricTypeParams(
                measure=PydanticMetricInputMeasure(name="revenue", filter=_filter("status = 'paid'")),
            ),
            filter=None,
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[metric]))

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        assert metrics[0].expression.dialects[0].expression == "SUM(CASE WHEN status = 'paid' THEN amount END)"

    def test_metric_and_measure_filters_combined_with_and(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("revenue", agg=AggregationType.SUM, expr="amount")],
        )
        metric = PydanticMetric(
            name="paid_intl_revenue",
            description=None,
            type=MetricType.SIMPLE,
            type_params=PydanticMetricTypeParams(
                measure=PydanticMetricInputMeasure(name="revenue", filter=_filter("status = 'paid'")),
            ),
            filter=_filter("region = 'intl'"),
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[metric]))

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        assert metrics[0].expression.dialects[0].expression == (
            "SUM(CASE WHEN (region = 'intl') AND (status = 'paid') THEN amount END)"
        )

    def test_jinja_dimension_reference_rendered(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("revenue", agg=AggregationType.SUM, expr="amount")],
        )
        metric = PydanticMetric(
            name="us_revenue",
            description=None,
            type=MetricType.SIMPLE,
            type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name="revenue")),
            filter=_filter("{{ Dimension('order__country') }} = 'US'"),
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[metric]))

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        assert metrics[0].expression.dialects[0].expression == "SUM(CASE WHEN order__country = 'US' THEN amount END)"

    def test_ratio_metric_filter_propagated_to_both_sides(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[
                _measure("revenue", agg=AggregationType.SUM, expr="amount"),
                _measure("order_count", agg=AggregationType.COUNT, expr="order_id"),
            ],
        )
        revenue_m = _simple_metric("revenue", "revenue")
        order_count_m = _simple_metric("order_count", "order_count")
        arpu = PydanticMetric(
            name="paid_arpu",
            description=None,
            type=MetricType.RATIO,
            type_params=PydanticMetricTypeParams(
                numerator=PydanticMetricInput(name="revenue"),
                denominator=PydanticMetricInput(name="order_count"),
            ),
            filter=_filter("status = 'paid'"),
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[revenue_m, order_count_m, arpu]))

        assert result.semantic_model[0].metrics is not None
        paid_arpu = next(m for m in result.semantic_model[0].metrics if m.name == "paid_arpu")
        expr = paid_arpu.expression.dialects[0].expression
        assert expr == (
            "(SUM(CASE WHEN status = 'paid' THEN amount END))"
            " / "
            "(COUNT(CASE WHEN status = 'paid' THEN order_id END))"
        )

    def test_derived_metric_filter_propagated_to_sub_expressions(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[
                _measure("revenue", agg=AggregationType.SUM, expr="amount"),
                _measure("cost", agg=AggregationType.SUM, expr="cost_amount"),
            ],
        )
        revenue_m = _simple_metric("revenue", "revenue")
        cost_m = _simple_metric("cost", "cost")
        profit = PydanticMetric(
            name="paid_profit",
            description=None,
            type=MetricType.DERIVED,
            type_params=PydanticMetricTypeParams(
                expr="revenue - cost",
                metrics=[PydanticMetricInput(name="revenue"), PydanticMetricInput(name="cost")],
            ),
            filter=_filter("status = 'paid'"),
            metadata=default_meta(),
            config=None,
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm], metrics=[revenue_m, cost_m, profit]))

        assert result.semantic_model[0].metrics is not None
        paid_profit = next(m for m in result.semantic_model[0].metrics if m.name == "paid_profit")
        expr = paid_profit.expression.dialects[0].expression
        assert (
            expr
            == "SUM(CASE WHEN status = 'paid' THEN amount END) - SUM(CASE WHEN status = 'paid' THEN cost_amount END)"
        )

    def test_no_filter_produces_plain_expression(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            measures=[_measure("revenue", agg=AggregationType.SUM, expr="amount")],
        )
        result = MSIToOSIConverter().convert(
            _manifest(semantic_models=[sm], metrics=[_simple_metric("revenue", "revenue")])
        )

        metrics = result.semantic_model[0].metrics
        assert metrics is not None
        assert metrics[0].expression.dialects[0].expression == "SUM(amount)"


class TestOSIJsonSerialization:  # noqa: D101
    def test_to_osi_json_produces_valid_json(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(
            name="orders",
            description="Order data",
            dimensions=[_dimension("ds", dim_type=DimensionType.TIME, granularity=TimeGranularity.DAY)],
            measures=[_measure("revenue", expr="amount")],
            entities=[_entity("order_id", entity_type=EntityType.PRIMARY)],
        )
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]), model_name="my_project")
        parsed = json.loads(result.to_osi_json())

        assert parsed["version"] == "0.1.1"
        assert len(parsed["semantic_model"]) == 1
        assert parsed["semantic_model"][0]["name"] == "my_project"

    def test_to_osi_json_excludes_none_fields(self) -> None:  # noqa: D102
        sm = semantic_model_with_guaranteed_meta(name="orders")
        result = MSIToOSIConverter().convert(_manifest(semantic_models=[sm]))
        parsed = json.loads(result.to_osi_json())

        dataset = parsed["semantic_model"][0]["datasets"][0]
        assert "primary_key" not in dataset
        assert "unique_keys" not in dataset
        assert "fields" not in dataset
        assert "description" not in dataset
