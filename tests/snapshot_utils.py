from __future__ import annotations

import logging
import re
from typing import Any, Callable, Optional, Tuple, TypeVar

import tabulate
from _pytest.fixtures import FixtureRequest

from metricflow.dataflow.dataflow_plan import DataflowPlan
from metricflow.execution.execution_plan import ExecutionPlan
from metricflow.protocols.sql_client import SqlClient, SqlEngine
from metricflow.semantics.dag.mf_dag import MetricFlowDag
from metricflow.semantics.mf_logging.pretty_print import mf_pformat
from metricflow.semantics.model.semantics.linkable_spec_resolver import LinkableElementSet
from metricflow.semantics.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow.semantics.specs.spec_classes import InstanceSpecSet, LinkableSpecSet
from metricflow.semantics.test_helpers import assert_snapshot_text_equal
from tests.fixtures.setup_fixtures import MetricFlowTestConfiguration, check_sql_engine_snapshot_marker

logger = logging.getLogger(__name__)


# In plan outputs, replace strings that vary from run to run with this so that comparisons can be made
# consistently.
PLACEHOLDER_CHAR_FOR_INCOMPARABLE_STRINGS = "*"

# Needed as the table alias can vary from run to run.
_EXCLUDE_TABLE_ALIAS_REGEX = "^.*_src.*$"


def make_schema_replacement_function(system_schema: str, source_schema: str) -> Callable[[str], str]:
    """Generates a function to replace schema names in test outputs."""

    # The schema of the warehouse used in tests changes from run to run, so don't compare those.
    def replacement_function(text: str) -> str:
        # Replace with a string of the same length so that indents are preserved.
        text = text.replace(source_schema, PLACEHOLDER_CHAR_FOR_INCOMPARABLE_STRINGS * len(source_schema))
        # Same with the MetricFlow system schema.
        return text.replace(system_schema, PLACEHOLDER_CHAR_FOR_INCOMPARABLE_STRINGS * len(system_schema))

    return replacement_function


def replace_dataset_id_hash(text: str) -> str:
    """Replaces data set ID hashes for primed semantic models.

    The data set ID hash changes from run to run because it's based on the DW schema in the semantic model, which changes
    run to run.
    """
    pattern = re.compile(r"'[a-zA-Z0-9_]+__[a-zA-Z0-9_]+__(?P<hash>[a-zA-Z0-9_]+)'")
    while True:
        match = pattern.search(text)
        if match:
            data_set_id_hash = match.group("hash")
            # Replace with the same length to preserve indents
            text = text.replace(data_set_id_hash, PLACEHOLDER_CHAR_FOR_INCOMPARABLE_STRINGS * len(data_set_id_hash))
        else:
            break
    return text


PlanT = TypeVar("PlanT", bound=MetricFlowDag)


def assert_plan_snapshot_text_equal(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    plan: PlanT,
    plan_snapshot_text: str,
    plan_snapshot_file_extension: str = ".xml",
    exclude_line_regex: Optional[str] = None,
    incomparable_strings_replacement_function: Optional[Callable[[str], str]] = None,
    additional_sub_directories_for_snapshots: Tuple[str, ...] = (),
) -> None:
    """Checks if the given plan text is equal to the one that's saved for comparison.

    * The location of the file is automatically generated based on the test and the plan's ID.
    * This may create a new saved plan file or overwrite the existing one, depending on the configuration.
    * replace_incomparable_strings is used to replace strings in the plan text before comparison. Useful for making
      plans consistent when there are strings that vary between runs and shouldn't be compared.
    * additional_sub_directories_for_snapshots is used to specify additional sub-directories (in the automatically
      generated directory) where plan outputs should reside.

    TODO: Make this more generic by renaming plan -> DAG.
    """
    assert_snapshot_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        group_id=plan.__class__.__name__,
        snapshot_id=str(plan.dag_id),
        snapshot_text=plan_snapshot_text,
        snapshot_file_extension=plan_snapshot_file_extension,
        exclude_line_regex=exclude_line_regex,
        incomparable_strings_replacement_function=incomparable_strings_replacement_function,
        additional_sub_directories_for_snapshots=additional_sub_directories_for_snapshots,
    )


def assert_execution_plan_text_equal(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
    execution_plan: ExecutionPlan,
) -> None:
    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        plan=execution_plan,
        plan_snapshot_text=execution_plan.structure_text(),
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_configuration.mf_system_schema,
            source_schema=mf_test_configuration.mf_source_schema,
        ),
        additional_sub_directories_for_snapshots=(sql_client.sql_engine_type.value,),
    )


def assert_dataflow_plan_text_equal(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan: DataflowPlan,
    sql_client: SqlClient,
) -> None:
    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan.structure_text(),
        incomparable_strings_replacement_function=replace_dataset_id_hash,
        additional_sub_directories_for_snapshots=(sql_client.sql_engine_type.value,),
    )


def assert_object_snapshot_equal(  # type: ignore[misc]
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    obj_id: str,
    obj: Any,
    sql_client: Optional[SqlClient] = None,
) -> None:
    """For tests to compare large objects, this can be used to snapshot a text representation of the object."""
    if sql_client is not None:
        check_sql_engine_snapshot_marker(request)

    assert_snapshot_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        group_id=obj.__class__.__name__,
        snapshot_id=obj_id,
        snapshot_text=mf_pformat(obj),
        snapshot_file_extension=".txt",
        additional_sub_directories_for_snapshots=(sql_client.sql_engine_type.value,) if sql_client else (),
    )


def assert_sql_snapshot_equal(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    snapshot_id: str,
    sql: str,
    sql_engine: Optional[SqlEngine] = None,
) -> None:
    """For tests that generate SQL, use this to write / check snapshots."""
    if sql_engine is not None:
        check_sql_engine_snapshot_marker(request)

    assert_snapshot_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        group_id=sql.__class__.__name__,
        snapshot_id=snapshot_id,
        snapshot_text=sql,
        snapshot_file_extension=".sql",
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_configuration.mf_system_schema, source_schema=mf_test_configuration.mf_source_schema
        ),
        exclude_line_regex=_EXCLUDE_TABLE_ALIAS_REGEX,
        additional_sub_directories_for_snapshots=(sql_engine.value,) if sql_engine is not None else (),
    )


def assert_str_snapshot_equal(  # type: ignore[misc]
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    snapshot_id: str,
    snapshot_str: str,
    sql_engine: Optional[SqlEngine] = None,
) -> None:
    """Write / compare a string snapshot."""
    if sql_engine is not None:
        check_sql_engine_snapshot_marker(request)

    assert_snapshot_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        group_id=snapshot_str.__class__.__name__,
        snapshot_id=snapshot_id,
        snapshot_text=snapshot_str,
        snapshot_file_extension=".txt",
        additional_sub_directories_for_snapshots=(sql_engine.value,) if sql_engine is not None else (),
    )


def assert_linkable_element_set_snapshot_equal(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    set_id: str,
    linkable_element_set: LinkableElementSet,
) -> None:
    headers = ("Semantic Model", "Entity Links", "Name", "Time Granularity", "Date Part", "Properties")
    rows = []
    for linkable_dimension_iterable in linkable_element_set.path_key_to_linkable_dimensions.values():
        for linkable_dimension in linkable_dimension_iterable:
            rows.append(
                (
                    # Checking a limited set of fields as the result is large due to the paths in the object.
                    (
                        linkable_dimension.semantic_model_origin.semantic_model_name
                        if linkable_dimension.semantic_model_origin
                        else None
                    ),
                    tuple(entity_link.element_name for entity_link in linkable_dimension.entity_links),
                    linkable_dimension.element_name,
                    linkable_dimension.time_granularity.name if linkable_dimension.time_granularity is not None else "",
                    linkable_dimension.date_part.name if linkable_dimension.date_part is not None else "",
                    sorted(
                        linkable_element_property.name for linkable_element_property in linkable_dimension.properties
                    ),
                )
            )

    for linkable_entity_iterable in linkable_element_set.path_key_to_linkable_entities.values():
        for linkable_entity in linkable_entity_iterable:
            rows.append(
                (
                    # Checking a limited set of fields as the result is large due to the paths in the object.
                    linkable_entity.semantic_model_origin.semantic_model_name,
                    tuple(entity_link.element_name for entity_link in linkable_entity.entity_links),
                    linkable_entity.element_name,
                    "",
                    "",
                    sorted(linkable_element_property.name for linkable_element_property in linkable_entity.properties),
                )
            )

    for linkable_metric_iterable in linkable_element_set.path_key_to_linkable_metrics.values():
        for linkable_metric in linkable_metric_iterable:
            rows.append(
                (
                    # Checking a limited set of fields as the result is large due to the paths in the object.
                    linkable_metric.join_by_semantic_model.semantic_model_name,
                    tuple(entity_link.element_name for entity_link in linkable_entity.entity_links),
                    linkable_metric.element_name,
                    "",
                    "",
                    sorted(linkable_element_property.name for linkable_element_property in linkable_metric.properties),
                )
            )

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id=set_id,
        snapshot_str=tabulate.tabulate(headers=headers, tabular_data=sorted(rows)),
    )


def assert_spec_set_snapshot_equal(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, set_id: str, spec_set: InstanceSpecSet
) -> None:
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id=set_id,
        obj=sorted(spec.qualified_name for spec in spec_set.all_specs),
    )


def assert_linkable_spec_set_snapshot_equal(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, set_id: str, spec_set: LinkableSpecSet
) -> None:
    # TODO: This will be used in a later PR and this message will be removed.
    naming_scheme = ObjectBuilderNamingScheme()
    assert_snapshot_text_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        group_id=spec_set.__class__.__name__,
        snapshot_id=set_id,
        snapshot_text=mf_pformat(sorted(naming_scheme.input_str(spec) for spec in spec_set.as_tuple)),
        snapshot_file_extension=".txt",
        additional_sub_directories_for_snapshots=(),
    )
