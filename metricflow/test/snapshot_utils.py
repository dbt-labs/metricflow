from __future__ import annotations

import difflib
import logging
import os
import re
import webbrowser
from typing import Any, Callable, List, Optional, Tuple, TypeVar

import tabulate
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.pretty_print import pformat_big_objects

from metricflow.dag.mf_dag import MetricFlowDag
from metricflow.dataflow.dataflow_plan import DataflowPlan
from metricflow.dataflow.dataflow_plan_to_text import dataflow_plan_as_text
from metricflow.execution.execution_plan import ExecutionPlan
from metricflow.execution.execution_plan_to_text import execution_plan_to_text
from metricflow.model.semantics.linkable_spec_resolver import LinkableElementSet
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs.specs import InstanceSpecSet
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState

logger = logging.getLogger(__name__)


# In plan outputs, replace strings that vary from run to run with this so that comparisons can be made
# consistently.
PLACEHOLDER_CHAR_FOR_INCOMPARABLE_STRINGS = "*"


def make_schema_replacement_function(system_schema: str, source_schema: str) -> Callable[[str], str]:
    """Generates a function to replace schema names in test outputs."""

    # The schema of the warehouse used in tests changes from run to run, so don't compare those.
    def replacement_function(text: str) -> str:  # noqa: D
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


def snapshot_path_prefix(
    request: FixtureRequest,
    snapshot_group: str,
    snapshot_id: str,
    additional_sub_directories: Tuple[str, ...] = (),
) -> str:
    """Returns a path prefix that can be used to build filenames for files associated with the snapshot.

    The snapshot prefix is generated from the name of the test file, the name of the test, name of the snapshot class,
    and the name of the snapshot.

    e.g.
    .../snapshots/test_file.py/DataflowPlan/test_name__plan1

    which can be used to construct paths like

    .../snapshots/test_file.py/DataflowPlan/test_name__plan1.xml
    .../snapshots/test_file.py/DataflowPlan/test_name__plan1.svg
    """
    test_name = request.node.name
    snapshot_file_name = test_name + "__" + snapshot_id
    path_items: List[str] = []

    test_file_path_items = os.path.normpath(request.node.fspath).split(os.sep)
    test_file_name = test_file_path_items[-1]
    # Default to where this is defined, but use more appropriate directories if found.
    test_directory_root_index = -1
    for i, path_item in enumerate(test_file_path_items):
        if path_item in ("test", "metricflow", "metricflow_extensions"):
            test_directory_root_index = i + 1

    path_to_store_snapshots = os.sep.join(test_file_path_items[:test_directory_root_index])
    path_items.extend([path_to_store_snapshots, "snapshots", test_file_name, snapshot_group])

    if additional_sub_directories:
        path_items.extend(additional_sub_directories)
    path_items.append(snapshot_file_name)

    return os.path.abspath(os.path.join(*path_items))


PlanT = TypeVar("PlanT", bound=MetricFlowDag)


def _exclude_lines_matching_regex(file_contents: str, exclude_line_regex: str) -> str:
    """Removes lines from file contents if the line matches exclude_regex."""
    compiled_regex = re.compile(exclude_line_regex)
    return "\n".join([line for line in file_contents.split("\n") if not compiled_regex.match(line)])


def assert_plan_snapshot_text_equal(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
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
    """
    assert_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        group_id=plan.__class__.__name__,
        snapshot_id=plan.dag_id,
        snapshot_text=plan_snapshot_text,
        snapshot_file_extension=plan_snapshot_file_extension,
        exclude_line_regex=exclude_line_regex,
        incomparable_strings_replacement_function=incomparable_strings_replacement_function,
        additional_sub_directories_for_snapshots=additional_sub_directories_for_snapshots,
    )


def assert_snapshot_text_equal(
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    group_id: str,
    snapshot_id: str,
    snapshot_text: str,
    snapshot_file_extension: str,
    exclude_line_regex: Optional[str] = None,
    incomparable_strings_replacement_function: Optional[Callable[[str], str]] = None,
    additional_sub_directories_for_snapshots: Tuple[str, ...] = (),
) -> None:
    """Similar to assert_plan_snapshot_text_equal(), but with more controls on how the snapshot paths are generated."""
    file_path = (
        snapshot_path_prefix(
            request=request,
            snapshot_group=group_id,
            snapshot_id=snapshot_id,
            additional_sub_directories=additional_sub_directories_for_snapshots,
        )
        + snapshot_file_extension
    )

    if incomparable_strings_replacement_function is not None:
        snapshot_text = incomparable_strings_replacement_function(snapshot_text)

    # If we are in overwrite mode, make a new plan:
    if mf_test_session_state.overwrite_snapshots:
        # Create parent directory for the plan text files.
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as snapshot_text_file:
            snapshot_text_file.write(snapshot_text)
            # Add a new line at the end of the file so that PRSs don't show the "no newline" symbol on Github.
            snapshot_text_file.write("\n")

    # Throw an exception if the plan is not there.
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"Could not find snapshot file at path {file_path}. Re-run with --overwrite-snapshots and check git status "
            "to see what's new."
        )

    if mf_test_session_state.display_plans:
        if not mf_test_session_state.overwrite_snapshots:
            logger.warning(f"Not overwriting snapshots, so displaying existing snapshot at {file_path}")

        if mf_test_session_state.plans_displayed >= mf_test_session_state.max_plans_displayed:
            raise RuntimeError(
                f"Can't display snapshot - hit limit of "
                f"{mf_test_session_state.max_plans_displayed} "
                f"plans displayed."
            )
        webbrowser.open("file://" + file_path)
        mf_test_session_state.plans_displayed += 1

    # Read the existing plan from the file and compare with the actual plan
    with open(file_path, "r") as snapshot_text_file:
        # Remove the newline that was added from above.
        expected_snapshot_text = snapshot_text_file.read().rstrip()

        if exclude_line_regex:
            # Filter out lines that should be ignored.
            expected_snapshot_text = _exclude_lines_matching_regex(
                file_contents=expected_snapshot_text, exclude_line_regex=exclude_line_regex
            )
            snapshot_text = _exclude_lines_matching_regex(
                file_contents=snapshot_text, exclude_line_regex=exclude_line_regex
            )
        # pytest should show a detailed diff with "assert actual_modified == expected_modified", but it's not, so doing
        # this instead.
        if snapshot_text != expected_snapshot_text:
            differ = difflib.Differ()
            diff = differ.compare(expected_snapshot_text.splitlines(), snapshot_text.splitlines())
            assert False, f"Snapshot from {file_path} does not match. Diff from expected to actual:\n" + "\n".join(diff)


def assert_execution_plan_text_equal(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    sql_client: SqlClient,
    execution_plan: ExecutionPlan,
) -> None:
    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=execution_plan,
        plan_snapshot_text=execution_plan_to_text(execution_plan),
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_session_state.mf_system_schema,
            source_schema=mf_test_session_state.mf_source_schema,
        ),
        additional_sub_directories_for_snapshots=(sql_client.sql_engine_type.value,),
    )


def assert_dataflow_plan_text_equal(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    dataflow_plan: DataflowPlan,
    sql_client: SqlClient,
) -> None:
    assert_plan_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        plan=dataflow_plan,
        plan_snapshot_text=dataflow_plan_as_text(dataflow_plan),
        incomparable_strings_replacement_function=replace_dataset_id_hash,
        additional_sub_directories_for_snapshots=(sql_client.sql_engine_type.value,),
    )


def assert_object_snapshot_equal(  # type: ignore[misc]
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    obj_id: str,
    obj: Any,
) -> None:
    """For tests to compare large objects, this can be used to snapshot a text representation of the object."""
    assert_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        group_id=obj.__class__.__name__,
        snapshot_id=obj_id,
        snapshot_text=pformat_big_objects(obj),
        snapshot_file_extension=".txt",
    )


def assert_linkable_element_set_snapshot_equal(  # noqa: D
    request: FixtureRequest,
    mf_test_session_state: MetricFlowTestSessionState,
    set_id: str,
    linkable_element_set: LinkableElementSet,
) -> None:
    headers = ("Semantic Model", "Entity Links", "Name", "Time Granularity", "Properties")
    rows = []
    for linkable_dimension_iterable in linkable_element_set.path_key_to_linkable_dimensions.values():
        for linkable_dimension in linkable_dimension_iterable:
            rows.append(
                (
                    # Checking a limited set of fields as the result is large due to the paths in the object.
                    linkable_dimension.semantic_model_origin.semantic_model_name,
                    tuple(entity_link.element_name for entity_link in linkable_dimension.entity_links),
                    linkable_dimension.element_name,
                    linkable_dimension.time_granularity.name if linkable_dimension.time_granularity is not None else "",
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
                    sorted(linkable_element_property.name for linkable_element_property in linkable_entity.properties),
                )
            )
    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id=set_id,
        obj=tabulate.tabulate(headers=headers, tabular_data=sorted(rows)),
    )


def assert_spec_set_snapshot_equal(  # noqa: D
    request: FixtureRequest, mf_test_session_state: MetricFlowTestSessionState, set_id: str, spec_set: InstanceSpecSet
) -> None:
    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id=set_id,
        obj=sorted(spec.qualified_name for spec in spec_set.all_specs),
    )
