from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.references import SemanticModelReference

from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.snapshot_utils import assert_spec_set_snapshot_equal
from metricflow.test.sql.compare_sql_plan import assert_rendered_sql_equal

logger = logging.getLogger(__name__)


def test_convert_table_semantic_model_without_measures(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    consistent_id_object_repository: ConsistentIdObjectRepository,
) -> None:
    """Simple test for converting a table semantic model. Since there are no measures, primary time is not checked."""
    users_data_set = consistent_id_object_repository.simple_model_data_sets["users_latest"]

    assert_spec_set_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        set_id="result0",
        spec_set=users_data_set.instance_set.spec_set,
    )
    assert users_data_set.semantic_model_reference == SemanticModelReference(semantic_model_name="users_latest")
    assert_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan_id="plan0",
        select_node=users_data_set.sql_select_node,
        sql_client=sql_client,
    )


def test_convert_table_semantic_model_with_measures(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    consistent_id_object_repository: ConsistentIdObjectRepository,
) -> None:
    """Complete test of table semantic model conversion. This includes the full set of measures/entities/dimensions.

    Measures trigger a primary time dimension validation. Additionally, this includes both categorical and time
    dimension types, which should cover most, if not all, of the table source branches in the target class.
    """
    id_verifications_data_set = consistent_id_object_repository.simple_model_data_sets["id_verifications"]

    assert_spec_set_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        set_id="result0",
        spec_set=id_verifications_data_set.instance_set.spec_set,
    )

    assert id_verifications_data_set.semantic_model_reference == SemanticModelReference(
        semantic_model_name="id_verifications"
    )
    assert_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan_id="plan0",
        select_node=id_verifications_data_set.sql_select_node,
        sql_client=sql_client,
    )


def test_convert_query_semantic_model(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    default_sql_plan_renderer: SqlQueryPlanRenderer,
    consistent_id_object_repository: ConsistentIdObjectRepository,
) -> None:
    bookings_data_set = consistent_id_object_repository.simple_model_data_sets["revenue"]

    assert_rendered_sql_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan_id="plan0",
        select_node=bookings_data_set.sql_select_node,
        sql_client=sql_client,
    )
