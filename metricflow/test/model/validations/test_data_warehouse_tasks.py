from copy import deepcopy
from typing import Tuple

import pytest
from _pytest.fixtures import FixtureRequest

from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.model.data_warehouse_model_validator import (
    DataWarehouseModelValidator,
    DataWarehouseTaskBuilder,
    DataWarehouseValidationTask,
)
from metricflow.model.model_transformer import ModelTransformer
from metricflow.model.objects.data_source import Mutability, MutabilityType
from metricflow.model.objects.elements.dimension import Dimension, DimensionType
from metricflow.model.objects.elements.measure import AggregationType, Measure
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantic_model import SemanticModel
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.model.validations.helpers import data_source_with_guaranteed_meta
from metricflow.test.plan_utils import assert_snapshot_text_equal, make_schema_replacement_function
from metricflow.test.test_utils import as_datetime
from metricflow.test.time.configurable_time_source import ConfigurableTimeSource


@pytest.fixture(scope="module")
def mf_engine(  # noqa: D
    data_warehouse_validation_model: UserConfiguredModel,
    sql_client: SqlClient,
    time_spine_source: TimeSpineSource,
    mf_test_session_state: MetricFlowTestSessionState,
) -> MetricFlowEngine:
    semantic_model = SemanticModel(data_warehouse_validation_model)
    return MetricFlowEngine(
        semantic_model=semantic_model,
        sql_client=sql_client,
        column_association_resolver=DefaultColumnAssociationResolver(semantic_model=semantic_model),
        time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
        time_spine_source=time_spine_source,
        system_schema=mf_test_session_state.mf_system_schema,
    )


def test_build_data_source_tasks(
    mf_test_session_state: MetricFlowTestSessionState, data_warehouse_validation_model: UserConfiguredModel
) -> None:  # noqa:D
    tasks = DataWarehouseTaskBuilder.gen_data_source_tasks(model=data_warehouse_validation_model)
    assert len(tasks) == len(data_warehouse_validation_model.data_sources)
    (query_string, _params) = tasks[0].query_and_params_callable()
    assert (
        query_string
        == f"SELECT (true) AS col0 FROM (SELECT * FROM {mf_test_session_state.mf_source_schema}.fct_animals) AS source0 "
        f"WHERE is_dog IS NOT NULL"
    )


def test_task_runner(sql_client: SqlClient, mf_engine: MetricFlowEngine) -> None:  # noqa: D
    dw_validator = DataWarehouseModelValidator(sql_client=sql_client, mf_engine=mf_engine)

    def good_query() -> Tuple[str, SqlBindParameters]:
        return ("SELECT 'foo' AS foo", SqlBindParameters())

    tasks = [
        DataWarehouseValidationTask(query_and_params_callable=good_query, error_message="Could not select foo"),
    ]

    issues = dw_validator.run_tasks(tasks=tasks)
    assert len(issues.all_issues) == 0

    def bad_query() -> Tuple[str, SqlBindParameters]:
        return ("SELECT (true) AS col1 FROM doesnt_exist", SqlBindParameters())

    err_msg_bad = "Could not access table 'doesnt_exist' in data warehouse"
    bad_task = DataWarehouseValidationTask(query_and_params_callable=bad_query, error_message=err_msg_bad)

    tasks.append(bad_task)
    issues = dw_validator.run_tasks(tasks=tasks)
    assert len(issues.all_issues) == 1
    assert len(issues.errors) == 1
    assert err_msg_bad in issues.errors[0].message


def test_validate_data_sources(  # noqa: D
    data_warehouse_validation_model: UserConfiguredModel,
    create_data_warehouse_validation_model_tables: bool,
    sql_client: SqlClient,
    mf_engine: MetricFlowEngine,
) -> None:
    model = deepcopy(data_warehouse_validation_model)

    dw_validator = DataWarehouseModelValidator(sql_client=sql_client, mf_engine=mf_engine)

    issues = dw_validator.validate_data_sources(model)
    assert len(issues.all_issues) == 0

    model.data_sources.append(
        data_source_with_guaranteed_meta(
            name="test_data_source2",
            sql_table="doesnt_exist",
            dimensions=[],
            mutability=Mutability(type=MutabilityType.IMMUTABLE),
        )
    )

    issues = dw_validator.validate_data_sources(model)
    assert len(issues.all_issues) == 1
    assert len(issues.errors) == 1
    assert "Unable to access data source `test_data_source2`" in issues.all_issues[0].message


def test_build_dimension_tasks(  # noqa: D
    data_warehouse_validation_model: UserConfiguredModel, mf_test_session_state: MetricFlowTestSessionState
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_dimension_tasks(model=data_warehouse_validation_model)
    assert len(tasks) == 1  # on data source query with all dimensions
    assert len(tasks[0].on_fail_subtasks) == 2  # a sub task for each dimension on the data source
    (query_string, _params) = tasks[0].query_and_params_callable()
    assert (
        f"SELECT (ds) AS col0, (is_dog) AS col1 FROM (SELECT * FROM {mf_test_session_state.mf_source_schema}.fct_animals) AS source0 WHERE is_dog IS NOT NULL"
        == query_string
    )
    (query_string, _params) = tasks[0].on_fail_subtasks[0].query_and_params_callable()
    assert (
        f"SELECT (ds) AS col0 FROM (SELECT * FROM {mf_test_session_state.mf_source_schema}.fct_animals) AS source0 WHERE is_dog IS NOT NULL"
        == query_string
    )
    (query_string, _params) = tasks[0].on_fail_subtasks[1].query_and_params_callable()
    assert (
        f"SELECT (is_dog) AS col0 FROM (SELECT * FROM {mf_test_session_state.mf_source_schema}.fct_animals) AS source0 WHERE is_dog IS NOT NULL"
        == query_string
    )


def test_validate_dimensions(  # noqa: D
    data_warehouse_validation_model: UserConfiguredModel, sql_client: SqlClient, mf_engine: MetricFlowEngine
) -> None:
    model = deepcopy(data_warehouse_validation_model)

    dw_validator = DataWarehouseModelValidator(sql_client=sql_client, mf_engine=mf_engine)

    issues = dw_validator.validate_dimensions(model)
    assert len(issues.all_issues) == 0

    dimensions = list(model.data_sources[0].dimensions)
    dimensions.append(Dimension(name="doesnt_exist", type=DimensionType.CATEGORICAL))
    model.data_sources[0].dimensions = dimensions

    issues = dw_validator.validate_dimensions(model)
    # One isssue is created for the short circuit query failure, and another is
    # created for the sub task checking the specific dimension
    print("\nModelValidationResults", issues)
    assert len(issues.all_issues) == 2
    assert (
        f"Failed to query dimensions in data warehouse for data source `{model.data_sources[0].name}`"
        in issues.all_issues[0].message
    )
    assert (
        f"Unable to query `doesnt_exist` in data warehouse for dimension `doesnt_exist` on data source `{model.data_sources[0].name}`"
        in issues.all_issues[1].message
    )


def test_build_metric_tasks(  # noqa: D
    request: FixtureRequest,
    data_warehouse_validation_model: UserConfiguredModel,
    sql_client: SqlClient,
    mf_engine: MetricFlowEngine,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_metric_tasks(model=data_warehouse_validation_model, mf_engine=mf_engine)
    assert len(tasks) == 1
    (query_string, _params) = tasks[0].query_and_params_callable()
    assert_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        group_id="data_warehouse_validation_model",
        snapshot_id="query0",
        snapshot_text=query_string,
        snapshot_file_extension=".sql",
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_session_state.mf_system_schema, source_schema=mf_test_session_state.mf_source_schema
        ),
        additional_sub_directories_for_snapshots=(sql_client.__class__.__name__,),
    )


def test_validate_metrics(  # noqa: D
    data_warehouse_validation_model: UserConfiguredModel,
    sql_client: SqlClient,
    mf_engine: MetricFlowEngine,
    time_spine_source: TimeSpineSource,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    model = deepcopy(data_warehouse_validation_model)
    dw_validator = DataWarehouseModelValidator(sql_client=sql_client, mf_engine=mf_engine)

    issues = dw_validator.validate_metrics(model)
    assert len(issues.all_issues) == 0

    # Update model to have a new measure which creates a new metric by proxy
    new_measures = list(model.data_sources[0].measures)
    new_measures.append(
        Measure(
            name="count_cats",
            agg=AggregationType.SUM,
            expr="is_cat",  # doesn't exist as column
            create_metric=True,
        )
    )
    model.data_sources[0].measures = new_measures
    model.metrics = []
    model = ModelTransformer.pre_validation_transform_model(model)
    model = ModelTransformer.post_validation_transform_model(model)

    # Get new metric flow engine which has the context of the updated model
    semantic_model = SemanticModel(model)
    new_engine = MetricFlowEngine(
        semantic_model=semantic_model,
        sql_client=sql_client,
        column_association_resolver=DefaultColumnAssociationResolver(semantic_model=semantic_model),
        time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
        time_spine_source=time_spine_source,
        system_schema=mf_test_session_state.mf_system_schema,
    )

    # Validate new metric created by proxy causes an issue (because the column used doesn't exist)
    dw_validator = DataWarehouseModelValidator(sql_client=sql_client, mf_engine=new_engine)
    issues = dw_validator.validate_metrics(model)
    assert len(issues.all_issues) == 1
    assert len(issues.errors) == 1
    assert "Unable to query metric `count_cats`" in issues.errors[0].message
