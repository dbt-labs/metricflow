from __future__ import annotations

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow.integration.conftest import IntegrationTestHelpers
from tests_metricflow.snapshot_utils import assert_object_snapshot_equal


def test_list_dimensions_basic(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test basic listing of all dimensions."""
    dimensions = it_helpers.mf_engine.list_dimensions(sort=True)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[dim.qualified_name for dim in dimensions],
    )


def test_list_dimensions_with_search(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing dimensions with search filter."""
    filtered_dimensions = it_helpers.mf_engine.list_dimensions(search_str="country", sort=True)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[dim.qualified_name for dim in filtered_dimensions],
    )


def test_list_dimensions_with_pagination(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing dimensions with sorting and pagination."""
    # Get first page
    page_1_dimensions = it_helpers.mf_engine.list_dimensions(sort=True, page_num=1, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page1",
        obj=[dim.qualified_name for dim in page_1_dimensions],
    )

    # Get second page
    page_2_dimensions = it_helpers.mf_engine.list_dimensions(sort=True, page_num=2, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page2",
        obj=[dim.qualified_name for dim in page_2_dimensions],
    )

    # Verify pages are different
    page_1_names = set(dim.qualified_name for dim in page_1_dimensions)
    page_2_names = set(dim.qualified_name for dim in page_2_dimensions)
    assert not page_1_names.intersection(page_2_names), "Pages should contain different dimensions"


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


def test_list_metrics_with_search(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing metrics with search filter."""
    filtered_metrics = it_helpers.mf_engine.list_metrics(search_str="booking")
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[metric.name for metric in filtered_metrics],
    )


def test_list_metrics_with_pagination(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing metrics with pagination."""
    # Get first page
    page_1_metrics = it_helpers.mf_engine.list_metrics(page_num=1, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page1",
        obj=[metric.name for metric in page_1_metrics],
    )

    # Get second page
    page_2_metrics = it_helpers.mf_engine.list_metrics(page_num=2, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page2",
        obj=[metric.name for metric in page_2_metrics],
    )

    # Verify pages are different
    page_1_names = set(metric.name for metric in page_1_metrics)
    page_2_names = set(metric.name for metric in page_2_metrics)
    assert not page_1_names.intersection(page_2_names), "Pages should contain different metrics"


def test_simple_dimensions_for_single_metric(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test getting dimensions for a single metric."""
    single_metric_dims = it_helpers.mf_engine.simple_dimensions_for_metrics(["booking_value"])
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[dim.qualified_name for dim in single_metric_dims],
    )


def test_simple_dimensions_for_multiple_metrics(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test getting common dimensions for multiple metrics."""
    multi_metric_dims = it_helpers.mf_engine.simple_dimensions_for_metrics(["booking_value", "listings"])
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[dim.qualified_name for dim in multi_metric_dims],
    )


def test_simple_dimensions_with_search(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test getting dimensions with search filter."""
    filtered_dims = it_helpers.mf_engine.simple_dimensions_for_metrics(["booking_value"], search_str="country")
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[dim.qualified_name for dim in filtered_dims],
    )


def test_simple_dimensions_with_pagination(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test getting dimensions with sorting and pagination."""
    # Get first page
    page_1_dims = it_helpers.mf_engine.simple_dimensions_for_metrics(
        ["booking_value"], sort=True, page_num=1, page_size=2
    )
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page1",
        obj=[dim.qualified_name for dim in page_1_dims],
    )

    # Get second page
    page_2_dims = it_helpers.mf_engine.simple_dimensions_for_metrics(
        ["booking_value"], sort=True, page_num=2, page_size=2
    )
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page2",
        obj=[dim.qualified_name for dim in page_2_dims],
    )

    # Verify pages are different
    page_1_names = set(dim.qualified_name for dim in page_1_dims)
    page_2_names = set(dim.qualified_name for dim in page_2_dims)
    assert not page_1_names.intersection(page_2_names), "Pages should contain different dimensions"


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


def test_list_saved_queries_with_search(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing saved_queries with search filter."""
    filtered_saved_queries = it_helpers.mf_engine.list_saved_queries(search_str="booking")
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[saved_query.name for saved_query in filtered_saved_queries],
    )


def test_list_saved_queries_with_pagination(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing saved_queries with pagination."""
    # Get first page
    page_1_saved_queries = it_helpers.mf_engine.list_saved_queries(page_num=1, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page1",
        obj=[saved_query.name for saved_query in page_1_saved_queries],
    )

    # Get second page
    page_2_saved_queries = it_helpers.mf_engine.list_saved_queries(page_num=2, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page2",
        obj=[saved_query.name for saved_query in page_2_saved_queries],
    )

    # Verify pages are different
    page_1_names = set(saved_query.name for saved_query in page_1_saved_queries)
    page_2_names = set(saved_query.name for saved_query in page_2_saved_queries)
    assert not page_1_names.intersection(page_2_names), "Pages should contain different saved_queries"


def test_list_entities_basic(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test basic listing of all entities."""
    entities = it_helpers.mf_engine.list_entities(sort=True)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[entity.name for entity in entities],
    )


def test_list_entities_with_search(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing entities with search filter."""
    filtered_entities = it_helpers.mf_engine.list_entities(search_str="country", sort=True)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[entity.name for entity in filtered_entities],
    )


def test_list_entities_with_pagination(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing entities with sorting and pagination."""
    # Get first page
    page_1_entities = it_helpers.mf_engine.list_entities(sort=True, page_num=1, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page1",
        obj=[entity.name for entity in page_1_entities],
    )

    # Get second page
    page_2_entities = it_helpers.mf_engine.list_entities(sort=True, page_num=2, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page2",
        obj=[entity.name for entity in page_2_entities],
    )

    # Verify pages are different
    page_1_names = set(page_1_entities)
    page_2_names = set(page_2_entities)
    assert not page_1_names.intersection(page_2_names), "Pages should contain different entities"
