from __future__ import annotations

import itertools
import logging
from functools import cached_property
from typing import Optional, Sequence

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.semantic_graph.lookups.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.specs.where_filter.where_filter_spec_factory import WhereFilterSpecFactory
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat_dict
from metricflow_semantics.toolkit.string_helpers import mf_wrap

from metricflow.dataflow.optimizer.dataflow_optimizer_factory import DataflowPlanOptimization
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest, MetricFlowRequestId
from metricflow.metric_evaluation.me_plan_table_formatter import MetricEvaluationPlanTableFormatter
from metricflow.metric_evaluation.metric_query_planner import MetricEvaluationPlanner
from metricflow.metric_evaluation.plan.me_plan import MetricEvaluationPlan
from metricflow.plan_conversion.node_processor import PredicatePushdownState
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal

logger = logging.getLogger(__name__)


_TEST_CASE_ID_COUNTER = itertools.count()


def _get_next_request_id() -> MetricFlowRequestId:
    return MetricFlowRequestId(f"{next(_TEST_CASE_ID_COUNTER):02d}")


@fast_frozen_dataclass()
class MetricEvaluationTestCase:
    """Encapsulates cases for testing a `MetricEvaluationPlanner`."""

    case_id_suffix: str
    description: str
    request: MetricFlowQueryRequest
    expectation_description: Optional[str] = None

    @cached_property
    def case_id(self) -> str:  # noqa: D102
        return f"{self.request.request_id.mf_rid}__{self.case_id_suffix}"


METRIC_EVALUATION_TEST_CASES = (
    MetricEvaluationTestCase(
        case_id_suffix="simple_metric",
        description="Query for a simple metric.",
        request=MetricFlowQueryRequest.create(
            request_id=_get_next_request_id(),
            metric_names=["booking_value"],
            group_by_names=["metric_time"],
        ),
    ),
    MetricEvaluationTestCase(
        case_id_suffix="simple_metrics_from_common_model",
        description="Query for 2 simple metrics that are from the same model.",
        request=MetricFlowQueryRequest.create(
            request_id=_get_next_request_id(),
            metric_names=["booking_value", "bookings"],
            group_by_names=["metric_time"],
        ),
        expectation_description=mf_wrap(
            "For the passthrough planner: a single query can resolve both metrics since they are from the same"
            " model."
        ),
    ),
    MetricEvaluationTestCase(
        case_id_suffix="simple_metric_in_derived_metric",
        description="Query for a derived metric and one of its input metrics.",
        request=MetricFlowQueryRequest.create(
            request_id=_get_next_request_id(),
            metric_names=[
                "booking_value",
                "booking_fees",
            ],
            group_by_names=["metric_time"],
        ),
        expectation_description=mf_wrap(
            "For the passthrough planner: since `booking_fees` is derived from `booking_value`, the node that computes"
            " `booking_fees` can pass through `booking_value` and avoid the need for a FOJ in the top-level query."
        ),
    ),
    MetricEvaluationTestCase(
        case_id_suffix="filtered_simple_metric_in_derived_metric",
        description="Query for a simple metric and a derived metric that is defined using the same metric with a filter.",
        request=MetricFlowQueryRequest.create(
            request_id=_get_next_request_id(),
            metric_names=["booking_value", "instant_booking_value"],
            group_by_names=["metric_time"],
        ),
        expectation_description=mf_wrap(
            "For the passthrough planner: since `instant_booking_value` is computed from `booking_value` with a"
            " filter, a separate query is needed and passthrough can't be used to eliminate a join."
        ),
    ),
    MetricEvaluationTestCase(
        case_id_suffix="derived_metric_with_multiple_common_simple_metrics",
        description="Query for a derived metric and input metrics that are from different models",
        request=MetricFlowQueryRequest.create(
            request_id=_get_next_request_id(),
            metric_names=["bookings", "listings", "bookings_per_listing"],
            group_by_names=["metric_time"],
        ),
        expectation_description=mf_wrap(
            "For the passthrough planner: separate queries and a FOJ are needed to combine `bookings` and `listings`"
            " since they are defined in different models, but those metrics can be passed through to avoid a FOJ."
        ),
    ),
    MetricEvaluationTestCase(
        case_id_suffix="time_offset_common_simple_metric_with_custom_grain",
        description="Query for a simple metric and a derived metric that time-offsets the same simple metric."
        " Uses a custom grain in the group-by to help check query properties.",
        request=MetricFlowQueryRequest.create(
            request_id=_get_next_request_id(),
            metric_names=[
                "bookings",
                "bookings_offset_twice",
            ],
            group_by_names=["metric_time__day"],
        ),
        expectation_description=mf_wrap(
            "The input to `bookings_offset_twice` is defined with a time offset, which changes the query properties"
            " for the input. For the passthrough planner: `bookings` can't be passed through to the top-level query"
            " as there is a time offset."
        ),
    ),
    MetricEvaluationTestCase(
        case_id_suffix="derived_metrics_with_shared_source",
        description="Query for 2 derived metrics that can use the same source query.",
        request=MetricFlowQueryRequest.create(
            request_id=_get_next_request_id(),
            metric_names=["derived_bookings_0", "derived_bookings_1"],
            group_by_names=["metric_time"],
        ),
    ),
)


def _create_filter_spec_factory(
    query_spec: MetricFlowQuerySpec,
    manifest_object_lookup: ManifestObjectLookup,
    column_association_resolver: ColumnAssociationResolver,
) -> WhereFilterSpecFactory:
    return WhereFilterSpecFactory(
        column_association_resolver=column_association_resolver,
        spec_resolution_lookup=query_spec.filter_spec_resolution_lookup,
        custom_grain_names=tuple(grain.name for grain in manifest_object_lookup.expanded_time_grains),
    )


def assert_me_plan_snapshot_equal(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    me_plan: MetricEvaluationPlan,
    me_test_case: MetricEvaluationTestCase,
    sql: Optional[str],
) -> None:
    """Helper to compare snapshots for a metric evaluation plan."""
    format_result = MetricEvaluationPlanTableFormatter().format_plan(me_plan)

    return assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_str=mf_pformat_dict(
            description=me_test_case.description + "\n",
            obj_dict={
                "Metric Names": me_test_case.request.metric_names,
                "Group-By Names": me_test_case.request.group_by_names,
                "Metric Evaluation Overview": format_result.overview_table,
                "Metric Query Output": format_result.node_output_table,
                "SQL": sql,
            },
        ),
        expectation_description=me_test_case.expectation_description,
    )


def _create_top_level_query_filter_specs(
    filter_spec_factory: WhereFilterSpecFactory, query_spec: MetricFlowQuerySpec
) -> Sequence[WhereFilterSpec]:
    return tuple(
        filter_spec_factory.create_from_where_filter_intersection(
            filter_location=WhereFilterLocation.for_query(
                tuple(metric_spec.reference for metric_spec in query_spec.metric_specs)
            ),
            filter_intersection=query_spec.filter_intersection,
        )
    )


def check_metric_evaluation_plan_test_case(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    engine_test_fixture: MetricFlowEngineTestFixture,
    me_planner: MetricEvaluationPlanner,
    me_test_case: MetricEvaluationTestCase,
) -> None:
    """Check the test case for the given `MetricEvaluationPlanner`."""
    query_parser = engine_test_fixture.query_parser
    query_spec = query_parser.parse_and_validate_query(
        metric_names=me_test_case.request.metric_names,
        group_by_names=me_test_case.request.group_by_names,
    ).query_spec

    predicate_pushdown_state = PredicatePushdownState.create(
        time_range_constraint=query_spec.time_range_constraint,
        where_filter_specs=(),
    )
    me_plan = me_planner.build_plan(
        metric_specs=query_spec.metric_specs,
        group_by_item_specs=query_spec.linkable_specs.as_tuple,
        predicate_pushdown_state=predicate_pushdown_state,
        filter_spec_factory=_create_filter_spec_factory(
            query_spec=query_spec,
            manifest_object_lookup=engine_test_fixture.manifest_object_lookup,
            column_association_resolver=engine_test_fixture.column_association_resolver,
        ),
    )
    execution_plan = engine_test_fixture.dataflow_to_execution_converter.convert_to_execution_plan(
        dataflow_plan=engine_test_fixture.dataflow_plan_builder.build_plan(
            query_spec=query_spec,
            optimizations=DataflowPlanOptimization.enabled_optimizations(),
            me_plan_override=me_plan,
        )
    )
    sql_statements = [
        task.sql_statement for task in execution_plan.execution_plan.tasks if task.sql_statement is not None
    ]
    assert_me_plan_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        me_test_case=me_test_case,
        me_plan=me_plan,
        sql="\n".join(sql_statement.without_descriptions.sql for sql_statement in sql_statements),
    )
