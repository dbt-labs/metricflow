from __future__ import annotations

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.engine.metricflow_engine import GroupByOrderByAttribute
from tests_metricflow.integration.conftest import IntegrationTestHelpers
from tests_metricflow.snapshot_utils import assert_object_snapshot_equal


def test_list_dimensions(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test basic listing of all dimensions."""
    dimensions = it_helpers.mf_engine.list_dimensions()
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[dim.dunder_name for dim in dimensions],
    )


def test_list_metrics(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing all metrics."""
    metrics = it_helpers.mf_engine.list_metrics()

    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[metric.name for metric in metrics],
    )


def test_list_dimensions_for_metrics_for_single_metric(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test getting dimensions for a single metric."""
    single_metric_dims = it_helpers.mf_engine.list_dimensions(metric_names=["booking_value"])
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[dim.dunder_name for dim in single_metric_dims],
    )


def test_list_dimensions_for_metrics_for_multiple_metrics(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test getting common dimensions for multiple metrics."""
    multi_metric_dims = it_helpers.mf_engine.list_dimensions(metric_names=["booking_value", "listings"])
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[dim.dunder_name for dim in multi_metric_dims],
    )


def test_list_dimensions_order_by_semantic_model_name(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test getting dimensions for a single metric."""
    single_metric_dims = it_helpers.mf_engine.list_dimensions(order_by=GroupByOrderByAttribute.SEMANTIC_MODEL_NAME)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[
            (
                dim.semantic_model_reference.semantic_model_name if dim.semantic_model_reference else "",
                dim.dunder_name,
            )
            for dim in single_metric_dims
        ],
    )


def test_list_group_bys_order_by_semantic_model_name(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test getting group bys ordered by semantic model name."""
    group_bys = it_helpers.mf_engine.list_group_bys(order_by=GroupByOrderByAttribute.SEMANTIC_MODEL_NAME)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[
            (
                group_by.semantic_model_reference.semantic_model_name if group_by.semantic_model_reference else "",
                group_by.dunder_name if hasattr(group_by, "dunder_name") else group_by.name,
            )
            for group_by in group_bys
        ],
    )


def test_list_saved_queries(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing all saved_queries."""
    saved_queries = it_helpers.mf_engine.list_saved_queries()

    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[saved_query.name for saved_query in saved_queries],
    )


def test_entities_for_metrics(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing entities with metric filter."""
    entities = it_helpers.mf_engine.entities_for_metrics(metric_names=["bookings"])
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[
            (
                entity.semantic_model_reference.semantic_model_name,
                entity.name,
            )
            for entity in entities
        ],
    )


def test_list_group_bys(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test basic listing of all group bys."""
    group_bys = it_helpers.mf_engine.list_group_bys()
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[group_by.default_search_and_sort_attribute for group_by in group_bys],
    )


def test_list_group_bys_with_metric_names(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test basic listing of all group bys for a metric."""
    group_bys = it_helpers.mf_engine.list_group_bys(metric_names=["bookings"])
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[group_by.default_search_and_sort_attribute for group_by in group_bys],
    )


def test_group_by_exists(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test checking if group by elements exist in the semantic manifest."""
    # Test a dimension that should exist
    assert it_helpers.mf_engine.group_by_exists(
        StructuredLinkableSpecName(element_name="ds", entity_link_names=("booking",))
    )
    assert it_helpers.mf_engine.group_by_exists(
        StructuredLinkableSpecName(element_name="metric_time", entity_link_names=())
    )

    # Test an entity that should exist
    assert it_helpers.mf_engine.group_by_exists(
        StructuredLinkableSpecName(element_name="listing", entity_link_names=("listing",))
    )

    # Test a non-existent element
    assert not it_helpers.mf_engine.group_by_exists(
        StructuredLinkableSpecName(element_name="non_existent_element", entity_link_names=())
    )

    # Test with incorrect primary entity
    assert not it_helpers.mf_engine.group_by_exists(
        StructuredLinkableSpecName(element_name="ds", entity_link_names=("not_real_entity",))
    )
