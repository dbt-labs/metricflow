from __future__ import annotations

import logging
from typing import Any, Optional

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_plan_snapshot_text_equal,
    assert_snapshot_text_equal,
    make_schema_replacement_function,
    replace_dataset_id_hash,
)
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat

from metricflow.dataflow.dataflow_plan import DataflowPlan
from metricflow.execution.execution_plan import ExecutionPlan
from metricflow.protocols.sql_client import SqlClient, SqlEngine
from tests_metricflow.fixtures.setup_fixtures import check_sql_engine_snapshot_marker

logger = logging.getLogger(__name__)


# Needed as the table alias can vary from run to run.
_EXCLUDE_TABLE_ALIAS_REGEX = "^.*_src.*$"

# In the snapshot file, include a header with the name of the SQL engine when appropriate.
SQL_ENGINE_HEADER_NAME = "sql_engine"


def assert_execution_plan_text_equal(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    execution_plan: ExecutionPlan,
    expectation_description: Optional[str] = None,
) -> None:
    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=execution_plan,
        plan_snapshot_text=execution_plan.structure_text(),
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_configuration.mf_system_schema,
            source_schema=mf_test_configuration.mf_source_schema,
        ),
        additional_sub_directories_for_snapshots=(sql_client.sql_engine_type.value,),
        expectation_description=expectation_description,
    )


def assert_dataflow_plan_text_equal(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan: DataflowPlan,
    sql_client: SqlClient,
    expectation_description: Optional[str] = None,
) -> None:
    assert_plan_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
        incomparable_strings_replacement_function=replace_dataset_id_hash,
        additional_sub_directories_for_snapshots=(sql_client.sql_engine_type.value,),
        expectation_description=expectation_description,
    )


def assert_object_snapshot_equal(  # type: ignore[misc]
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    obj: Any,
    obj_id: str = "result",
    sql_client: Optional[SqlClient] = None,
    expectation_description: Optional[str] = None,
) -> None:
    """For tests to compare large objects, this can be used to snapshot a text representation of the object."""
    if sql_client is not None:
        check_sql_engine_snapshot_marker(request)

    assert_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        group_id=obj.__class__.__name__,
        snapshot_id=obj_id,
        snapshot_text=mf_pformat(obj),
        snapshot_file_extension=".txt",
        additional_sub_directories_for_snapshots=(sql_client.sql_engine_type.value,) if sql_client else (),
        expectation_description=expectation_description,
    )


def assert_sql_snapshot_equal(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql: str,
    snapshot_id: str = "result",
    sql_engine: Optional[SqlEngine] = None,
    expectation_description: Optional[str] = None,
) -> None:
    """For tests that generate SQL, use this to write / check snapshots."""
    if sql_engine is not None:
        check_sql_engine_snapshot_marker(request)

    assert_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        group_id=sql.__class__.__name__,
        snapshot_id=snapshot_id,
        snapshot_text=sql,
        snapshot_file_extension=".sql",
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_configuration.mf_system_schema, source_schema=mf_test_configuration.mf_source_schema
        ),
        exclude_line_regex=_EXCLUDE_TABLE_ALIAS_REGEX,
        additional_sub_directories_for_snapshots=(sql_engine.value,) if sql_engine is not None else (),
        additional_header_fields={SQL_ENGINE_HEADER_NAME: sql_engine.value} if sql_engine is not None else None,
        expectation_description=expectation_description,
    )


def assert_str_snapshot_equal(  # type: ignore[misc]
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    snapshot_str: str,
    snapshot_id: str = "result",
    snapshot_file_extension: str = ".txt",
    sql_engine: Optional[SqlEngine] = None,
    expectation_description: Optional[str] = None,
) -> None:
    """Write / compare a string snapshot."""
    if sql_engine is not None:
        check_sql_engine_snapshot_marker(request)

    assert_snapshot_text_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        group_id=snapshot_str.__class__.__name__,
        snapshot_id=snapshot_id,
        snapshot_text=snapshot_str,
        snapshot_file_extension=snapshot_file_extension,
        additional_sub_directories_for_snapshots=(sql_engine.value,) if sql_engine is not None else (),
        expectation_description=expectation_description,
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_configuration.mf_system_schema,
            source_schema=mf_test_configuration.mf_source_schema,
        ),
    )
