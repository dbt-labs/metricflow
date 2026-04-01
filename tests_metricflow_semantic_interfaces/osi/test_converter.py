from __future__ import annotations

import json

from metricflow.converters.osi.converter import MSIToOSIConverter, OSIToMSIConverter
from metricflow.converters.osi.models import (
    OSIDataset,
    OSIDialect,
    OSIDialectExpression,
    OSIDimension,
    OSIDocument,
    OSIExpression,
    OSIField,
    OSIMetric,
    OSIRelationship,
    OSISemanticModel,
)
from metricflow_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.elements.measure import PydanticMeasure
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
from metricflow_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_model import (
    PydanticNodeRelation,
    PydanticSemanticModel,
)
from metricflow_semantic_interfaces.test_utils import (
    default_meta,
    semantic_model_with_guaranteed_meta,
)
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)


def _manifest(
    semantic_models: list | None = None,
    metrics: list[PydanticMetric] | None = None,
) -> PydanticSemanticManifest:
    return PydanticSemanticManifest(
        semantic_models=semantic_models or [],
        metrics=metrics or [],
        project_configuration=PydanticProjectConfiguration(),
    )


def _simple_metric(
    name: str,
    measure_name: str,
    description: str | None = None,
) -> PydanticMetric:
    return PydanticMetric(
        name=name,
        description=description,
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            measure=PydanticMetricInputMeasure(name=measure_name),
        ),
        filter=None,
        metadata=default_meta(),
        config=None,
    )


def _dimension(
    name: str,
    dim_type: DimensionType = DimensionType.CATEGORICAL,
    expr: str | None = None,
    description: str | None = None,
    label: str | None = None,
    granularity: TimeGranularity | None = None,
) -> PydanticDimension:
    type_params = PydanticDimensionTypeParams(time_granularity=granularity) if granularity else None
    return PydanticDimension(
        name=name,
        type=dim_type,
        expr=expr,
        description=description,
        label=label,
        type_params=type_params,
        metadata=default_meta(),
        config=None,
    )


def _measure(
    name: str,
    agg: AggregationType = AggregationType.SUM,
    expr: str | None = None,
    description: str | None = None,
    label: str | None = None,
) -> PydanticMeasure:
    return PydanticMeasure(
        name=name,
        agg=agg,
        expr=expr,
        description=description,
        label=label,
        create_metric=None,
        agg_params=None,
        metadata=default_meta(),
    )


def _entity(
    name: str,
    entity_type: EntityType = EntityType.PRIMARY,
    expr: str | None = None,
) -> PydanticEntity:
    return PydanticEntity(
        name=name,
        type=entity_type,
        expr=expr,
        description=None,
        role=None,
        config=None,
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
        # bookings holds the FK (FOREIGN) → from; listings holds the PK (PRIMARY) → to
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
        # orders (FOREIGN) is always the from-side; users_a/users_b (PRIMARY/UNIQUE) are to-side.
        # users_a vs users_b are both one-side, so alphabetical tiebreaker applies.
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
        # bookings (FOREIGN) → from side; listings (PRIMARY) → to side
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
        # Name is {from_ds}__{to_ds}__{entity}: bookings (FOREIGN) is from, listings (PRIMARY) is to
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
        # beta (PRIMARY) appears first in the manifest, alpha (FOREIGN) appears second.
        # The converter must assign from=alpha (FK/many-side) and to=beta (PK/one-side),
        # ignoring manifest order.
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


# ---------------------------------------------------------------------------
# OSIToMSIConverter tests
# ---------------------------------------------------------------------------


def _osi_expr(expression: str, dialect: OSIDialect = OSIDialect.ANSI_SQL) -> OSIExpression:
    return OSIExpression(dialects=[OSIDialectExpression(dialect=dialect, expression=expression)])


def _osi_field(
    name: str,
    expression: str | None = None,
    is_time: bool | None = None,
    description: str | None = None,
    label: str | None = None,
) -> OSIField:
    return OSIField(
        name=name,
        expression=_osi_expr(expression if expression is not None else name),
        dimension=OSIDimension(is_time=is_time) if is_time is not None else None,
        description=description,
        label=label,
    )


def _osi_dataset(
    name: str,
    source: str = "schema.table",
    fields: list[OSIField] | None = None,
    primary_key: list[str] | None = None,
    unique_keys: list[list[str]] | None = None,
    description: str | None = None,
) -> OSIDataset:
    return OSIDataset(
        name=name,
        source=source,
        fields=fields,
        primary_key=primary_key,
        unique_keys=unique_keys,
        description=description,
    )


def _osi_metric(name: str, expression: str, description: str | None = None) -> OSIMetric:
    return OSIMetric(name=name, expression=_osi_expr(expression), description=description)


def _osi_relationship(
    name: str, from_dataset: str, to_dataset: str, from_columns: list[str], to_columns: list[str]
) -> OSIRelationship:
    return OSIRelationship(
        name=name,
        from_dataset=from_dataset,
        to=to_dataset,
        from_columns=from_columns,
        to_columns=to_columns,
    )


def _osi_doc(
    datasets: list[OSIDataset] | None = None,
    metrics: list[OSIMetric] | None = None,
    relationships: list[OSIRelationship] | None = None,
    model_name: str = "test",
) -> OSIDocument:
    return OSIDocument(
        semantic_model=[
            OSISemanticModel(
                name=model_name,
                datasets=datasets or [],
                metrics=metrics if metrics else None,
                relationships=relationships if relationships else None,
            )
        ]
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
        from metricflow_semantic_interfaces.type_enums import DimensionType

        assert dim.type == DimensionType.TIME
        assert dim.type_params is not None

    def test_is_time_false_becomes_categorical_dimension(self) -> None:  # noqa: D102
        doc = _osi_doc(datasets=[_osi_dataset("orders", fields=[_osi_field("status", is_time=False)])])
        sm = OSIToMSIConverter().convert(doc).semantic_models[0]

        assert len(sm.dimensions) == 1
        assert sm.dimensions[0].name == "status"
        from metricflow_semantic_interfaces.type_enums import DimensionType

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
        from metricflow_semantic_interfaces.type_enums import AggregationType

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
        from metricflow_semantic_interfaces.type_enums import MetricType

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
        # The measure should have COUNT_DISTINCT aggregation
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

        from metricflow_semantic_interfaces.type_enums import MetricType

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

        from metricflow_semantic_interfaces.type_enums import MetricType

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
        from metricflow_semantic_interfaces.type_enums import MetricType

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
