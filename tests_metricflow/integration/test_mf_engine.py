from __future__ import annotations

import math

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow.integration.conftest import IntegrationTestHelpers
from tests_metricflow.snapshot_utils import assert_object_snapshot_equal


def test_list_dimensions_basic(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test basic listing of all dimensions."""
    dimensions = it_helpers.mf_engine.list_dimensions_paginated()
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[dim.qualified_name for dim in dimensions.items],
    )
    assert dimensions.page_num == 1
    assert dimensions.page_size is None
    assert dimensions.total_items == len(dimensions.items)
    assert dimensions.total_pages == 1


def test_list_dimensions_with_search(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing dimensions with search filter."""
    filtered_dimensions = it_helpers.mf_engine.list_dimensions_paginated(search_str="country")
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[dim.qualified_name for dim in filtered_dimensions.items],
    )
    assert filtered_dimensions.page_num == 1
    assert filtered_dimensions.page_size is None
    assert filtered_dimensions.total_items == len(filtered_dimensions.items)
    assert filtered_dimensions.total_pages == 1


def test_list_dimensions_with_pagination(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing dimensions with sorting and pagination."""
    # Get first page
    page_1 = it_helpers.mf_engine.list_dimensions_paginated(page_num=1, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page1",
        obj=page_1,
    )
    all_dimensions = it_helpers.mf_engine.list_dimensions_paginated()
    assert page_1.page_num == 1
    assert page_1.page_size == 2
    assert page_1.total_items == len(all_dimensions.items)
    assert page_1.total_pages == math.ceil(page_1.total_items / page_1.page_size)

    # Get second page
    page_2 = it_helpers.mf_engine.list_dimensions_paginated(page_num=2, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page2",
        obj=page_2,
    )
    assert page_2.page_num == 2
    assert page_2.page_size == 2
    assert page_2.total_items == page_1.total_items
    assert page_2.total_pages == page_1.total_pages

    # Verify pages are different
    page_1_names = set(dim.qualified_name for dim in page_1.items)
    page_2_names = set(dim.qualified_name for dim in page_2.items)
    assert page_1_names != page_2_names, "Pages should contain different dimensions"


def test_list_metrics(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing all metrics."""
    metrics = it_helpers.mf_engine.list_metrics_paginated()

    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[metric.name for metric in metrics.items],
    )
    assert metrics.page_num == 1
    assert metrics.page_size is None
    assert metrics.total_items == len(metrics.items)
    assert metrics.total_pages == 1


def test_list_metrics_with_search(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing metrics with search filter."""
    filtered_metrics = it_helpers.mf_engine.list_metrics_paginated(search_str="booking")
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[metric.name for metric in filtered_metrics.items],
    )
    assert filtered_metrics.page_num == 1
    assert filtered_metrics.page_size is None
    assert filtered_metrics.total_items == len(filtered_metrics.items)
    assert filtered_metrics.total_pages == 1


def test_list_metrics_with_pagination(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing metrics with pagination."""
    # Get first page
    page_1 = it_helpers.mf_engine.list_metrics_paginated(page_num=1, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page1",
        obj=page_1,
    )
    all_metrics = it_helpers.mf_engine.list_metrics_paginated()
    assert page_1.page_num == 1
    assert page_1.page_size == 2
    assert page_1.total_items == len(all_metrics.items)
    assert page_1.total_pages == math.ceil(page_1.total_items / page_1.page_size)

    # Get second page
    page_2 = it_helpers.mf_engine.list_metrics_paginated(page_num=2, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page2",
        obj=page_2,
    )
    assert page_2.page_num == 2
    assert page_2.page_size == 2
    assert page_2.total_items == page_1.total_items
    assert page_2.total_pages == page_1.total_pages

    # Verify pages are different
    page_1_names = set(metric.name for metric in page_1.items)
    page_2_names = set(metric.name for metric in page_2.items)
    assert page_1_names != page_2_names, "Pages should contain different metrics"


def test_list_dimensions_for_metrics_for_single_metric(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test getting dimensions for a single metric."""
    single_metric_dims = it_helpers.mf_engine.list_dimensions_paginated(metric_names=["booking_value"])
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[dim.qualified_name for dim in single_metric_dims.items],
    )
    assert single_metric_dims.page_num == 1
    assert single_metric_dims.page_size is None
    assert single_metric_dims.total_items == len(single_metric_dims.items)
    assert single_metric_dims.total_pages == 1


def test_list_dimensions_for_metrics_for_multiple_metrics(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test getting common dimensions for multiple metrics."""
    multi_metric_dims = it_helpers.mf_engine.list_dimensions_paginated(metric_names=["booking_value", "listings"])
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[dim.qualified_name for dim in multi_metric_dims.items],
    )
    assert multi_metric_dims.page_num == 1
    assert multi_metric_dims.page_size is None
    assert multi_metric_dims.total_items == len(multi_metric_dims.items)
    assert multi_metric_dims.total_pages == 1


def test_list_dimensions_for_metrics_with_search(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test getting dimensions with search filter."""
    filtered_dims = it_helpers.mf_engine.list_dimensions_paginated(metric_names=["booking_value"], search_str="country")
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[dim.qualified_name for dim in filtered_dims.items],
    )
    assert filtered_dims.page_num == 1
    assert filtered_dims.page_size is None
    assert filtered_dims.total_items == len(filtered_dims.items)
    assert filtered_dims.total_pages == 1


def test_list_dimensions_for_metrics_with_pagination(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test getting dimensions with sorting and pagination."""
    # Get first page
    page_1 = it_helpers.mf_engine.list_dimensions_paginated(metric_names=["booking_value"], page_num=1, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page1",
        obj=[dim.qualified_name for dim in page_1.items],
    )
    all_dimensions = it_helpers.mf_engine.list_dimensions_paginated(metric_names=["booking_value"])
    assert page_1.page_num == 1
    assert page_1.page_size == 2
    assert page_1.total_items == len(all_dimensions.items)
    assert page_1.total_pages == math.ceil(page_1.total_items / page_1.page_size)

    # Get second page
    page_2 = it_helpers.mf_engine.list_dimensions_paginated(metric_names=["booking_value"], page_num=2, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page2",
        obj=[dim.qualified_name for dim in page_2.items],
    )
    assert page_2.page_num == 2
    assert page_2.page_size == 2
    assert page_2.total_items == page_1.total_items
    assert page_2.total_pages == page_1.total_pages

    # Verify pages are different
    page_1_names = set(dim.qualified_name for dim in page_1.items)
    page_2_names = set(dim.qualified_name for dim in page_2.items)
    assert page_1_names != page_2_names, "Pages should contain different dimensions"


def test_list_saved_queries(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing all saved_queries."""
    saved_queries = it_helpers.mf_engine.list_saved_queries_paginated()

    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[saved_query.name for saved_query in saved_queries.items],
    )
    assert saved_queries.page_num == 1
    assert saved_queries.page_size is None
    assert saved_queries.total_items == len(saved_queries.items)
    assert saved_queries.total_pages == 1


def test_list_saved_queries_with_search(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing saved_queries with search filter."""
    filtered_saved_queries = it_helpers.mf_engine.list_saved_queries_paginated(search_str="booking")
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[saved_query.name for saved_query in filtered_saved_queries.items],
    )
    assert filtered_saved_queries.page_num == 1
    assert filtered_saved_queries.page_size is None
    assert filtered_saved_queries.total_items == len(filtered_saved_queries.items)
    assert filtered_saved_queries.total_pages == 1


def test_list_saved_queries_with_pagination(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing saved_queries with pagination."""
    # Get first page
    page_1 = it_helpers.mf_engine.list_saved_queries_paginated(page_num=1, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page1",
        obj=page_1,
    )
    all_saved_queries = it_helpers.mf_engine.list_saved_queries_paginated()
    assert page_1.page_num == 1
    assert page_1.page_size == 2
    assert page_1.total_items == len(all_saved_queries.items)
    assert page_1.total_pages == math.ceil(page_1.total_items / page_1.page_size)

    # Get second page
    page_2 = it_helpers.mf_engine.list_saved_queries_paginated(page_num=2, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page2",
        obj=page_2,
    )
    assert page_2.page_num == 2
    assert page_2.page_size == 2
    assert page_2.total_items == page_1.total_items
    assert page_2.total_pages == page_1.total_pages

    # Verify pages are different
    page_1_names = set(saved_query.name for saved_query in page_1.items)
    page_2_names = set(saved_query.name for saved_query in page_2.items)
    assert not page_1_names.intersection(page_2_names), "Pages should contain different saved_queries"


def test_entities_for_metrics(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing entities with metric filter."""
    entities = it_helpers.mf_engine.list_entities_paginated(metric_names=["bookings"])
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[entity.name for entity in entities.items],
    )
    assert entities.page_num == 1
    assert entities.page_size is None
    assert entities.total_items == len(entities.items)
    assert entities.total_pages == 1


def test_entities_for_metrics_with_search(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing entities with search filter."""
    filtered_entities = it_helpers.mf_engine.list_entities_paginated(search_str="list", metric_names=["bookings"])
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[entity.name for entity in filtered_entities.items],
    )
    assert filtered_entities.page_num == 1
    assert filtered_entities.page_size is None
    assert filtered_entities.total_items == len(filtered_entities.items)
    assert filtered_entities.total_pages == 1


def test_entities_for_metrics_with_pagination(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing entities with sorting and pagination."""
    # Get first page
    page_1 = it_helpers.mf_engine.list_entities_paginated(metric_names=["bookings"], page_num=1, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page1",
        obj=[entity.name for entity in page_1.items],
    )
    all_entities = it_helpers.mf_engine.list_entities_paginated(metric_names=["bookings"])
    assert page_1.page_num == 1
    assert page_1.page_size == 2
    assert page_1.total_items == len(all_entities.items)
    assert page_1.total_pages == math.ceil(page_1.total_items / page_1.page_size)

    # Get second page
    page_2 = it_helpers.mf_engine.list_entities_paginated(metric_names=["bookings"], page_num=2, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page2",
        obj=[entity.name for entity in page_2.items],
    )
    assert page_2.page_num == 2
    assert page_2.page_size == 2
    assert page_2.total_items == page_1.total_items
    assert page_2.total_pages == page_1.total_pages

    # Verify pages are different
    page_1_names = set(entity.name for entity in page_1.items)
    page_2_names = set(entity.name for entity in page_2.items)
    assert not page_1_names.intersection(page_2_names), "Pages should contain different entities"


def test_list_group_bys_basic(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test basic listing of all group bys."""
    group_bys = it_helpers.mf_engine.list_group_bys_paginated()
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[group_by.default_search_and_sort_attribute for group_by in group_bys.items],
    )
    assert group_bys.page_num == 1
    assert group_bys.page_size is None
    assert group_bys.total_items == len(group_bys.items)
    assert group_bys.total_pages == 1


def test_list_group_bys_with_search(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing group bys with search filter."""
    filtered_group_bys = it_helpers.mf_engine.list_group_bys_paginated(search_str="user")
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[group_by.default_search_and_sort_attribute for group_by in filtered_group_bys.items],
    )
    assert filtered_group_bys.page_num == 1
    assert filtered_group_bys.page_size is None
    assert filtered_group_bys.total_items == len(filtered_group_bys.items)
    assert filtered_group_bys.total_pages == 1


def test_list_group_bys_with_pagination(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing group bys with sorting and pagination."""
    # Get first page
    page_1 = it_helpers.mf_engine.list_group_bys_paginated(page_num=1, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page1",
        obj=[group_by.default_search_and_sort_attribute for group_by in page_1.items],
    )
    all_group_bys = it_helpers.mf_engine.list_group_bys_paginated()
    assert page_1.page_num == 1
    assert page_1.page_size == 2
    assert page_1.total_items == len(all_group_bys.items)
    assert page_1.total_pages == math.ceil(page_1.total_items / page_1.page_size)

    # Get second page
    page_2 = it_helpers.mf_engine.list_group_bys_paginated(page_num=2, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page2",
        obj=[group_by.default_search_and_sort_attribute for group_by in page_2.items],
    )
    assert page_2.page_num == 2
    assert page_2.page_size == 2
    assert page_2.total_items == page_1.total_items
    assert page_2.total_pages == page_1.total_pages

    # Verify pages are different
    page_1_names = set(group_by.default_search_and_sort_attribute for group_by in page_1.items)
    page_2_names = set(group_by.default_search_and_sort_attribute for group_by in page_2.items)
    assert not page_1_names.intersection(page_2_names), "Pages should contain different group bys"


def test_list_group_bys_with_metric_names_basic(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test basic listing of all group bys for a metric."""
    group_bys = it_helpers.mf_engine.list_group_bys_paginated(metric_names=["bookings"])
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[group_by.default_search_and_sort_attribute for group_by in group_bys.items],
    )
    assert group_bys.page_num == 1
    assert group_bys.page_size is None
    assert group_bys.total_items == len(group_bys.items)
    assert group_bys.total_pages == 1


def test_list_group_bys_with_metric_names_and_search(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing group bys for a metric with search filter."""
    filtered_group_bys = it_helpers.mf_engine.list_group_bys_paginated(metric_names=["bookings"], search_str="user")
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=[group_by.default_search_and_sort_attribute for group_by in filtered_group_bys.items],
    )
    assert filtered_group_bys.page_num == 1
    assert filtered_group_bys.page_size is None
    assert filtered_group_bys.total_items == len(filtered_group_bys.items)
    assert filtered_group_bys.total_pages == 1


def test_list_group_bys_with_metric_names_and_pagination(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    """Test listing group bys for a metric with sorting and pagination."""
    # Get first page
    page_1 = it_helpers.mf_engine.list_group_bys_paginated(metric_names=["bookings"], page_num=1, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page1",
        obj=[group_by.default_search_and_sort_attribute for group_by in page_1.items],
    )
    all_group_bys = it_helpers.mf_engine.list_group_bys_paginated(metric_names=["bookings"])
    assert page_1.page_num == 1
    assert page_1.page_size == 2
    assert page_1.total_items == len(all_group_bys.items)
    assert page_1.total_pages == math.ceil(page_1.total_items / page_1.page_size)

    # Get second page
    page_2 = it_helpers.mf_engine.list_group_bys_paginated(metric_names=["bookings"], page_num=2, page_size=2)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="page2",
        obj=[group_by.default_search_and_sort_attribute for group_by in page_2.items],
    )
    assert page_2.page_num == 2
    assert page_2.page_size == 2
    assert page_2.total_items == page_1.total_items
    assert page_2.total_pages == page_1.total_pages

    # Verify pages are different
    page_1_names = set(group_by.default_search_and_sort_attribute for group_by in page_1.items)
    page_2_names = set(group_by.default_search_and_sort_attribute for group_by in page_2.items)
    assert not page_1_names.intersection(page_2_names), "Pages should contain different group bys"
