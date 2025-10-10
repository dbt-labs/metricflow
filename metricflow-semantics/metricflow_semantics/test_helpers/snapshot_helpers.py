from __future__ import annotations

import difflib
import logging
import os
import pathlib
import re
import webbrowser
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Tuple, TypeVar

import _pytest.fixtures
import tabulate
from _pytest.fixtures import FixtureRequest

from metricflow_semantics.dag.mf_dag import MetricFlowDag
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseGroupByItemSet
from metricflow_semantics.naming.object_builder_scheme import ObjectBuilderNamingScheme
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.spec_set import InstanceSpecSet, group_spec_by_type
from metricflow_semantics.test_helpers.terminal_helpers import mf_colored_link_text
from metricflow_semantics.toolkit.mf_logging.format_option import PrettyFormatOption
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.toolkit.string_helpers import mf_indent

logger = logging.getLogger(__name__)

# In the snapshot file, include a header that describe what should be observed in the snapshot.
SNAPSHOT_EXPECTATION_DESCRIPTION = "expectation_description"


@dataclass(frozen=True)
class SnapshotConfiguration:
    """Configuration for handling snapshots in a test session."""

    # Whether to display the snapshot associated with a test session in a browser window.
    display_snapshots: bool
    # Whether to overwrite any text files that were generated.
    overwrite_snapshots: bool
    # Absolute directory where the snapshots should be stored.
    snapshot_directory: pathlib.Path
    # Absolute directory where the tests are stored.
    tests_directory: pathlib.Path


def assert_snapshot_text_equal(
    request: _pytest.fixtures.FixtureRequest,
    snapshot_configuration: SnapshotConfiguration,
    group_id: str,
    snapshot_id: str,
    snapshot_text: str,
    snapshot_file_extension: str,
    exclude_line_regex: Optional[str] = None,
    incomparable_strings_replacement_function: Optional[Callable[[str], str]] = None,
    additional_sub_directories_for_snapshots: Tuple[str, ...] = (),
    additional_header_fields: Optional[Mapping[str, str]] = None,
    expectation_description: Optional[str] = None,
    include_headers: bool = True,
    log_snapshot_text: bool = True,
) -> None:
    """Similar to assert_plan_snapshot_text_equal(), but with more controls on how the snapshot paths are generated."""
    file_path = snapshot_path_prefix(
        request=request,
        snapshot_configuration=snapshot_configuration,
        snapshot_group=group_id,
        snapshot_id=snapshot_id,
        additional_sub_directories=additional_sub_directories_for_snapshots,
    ).with_suffix(snapshot_file_extension)

    if incomparable_strings_replacement_function is not None:
        snapshot_text = incomparable_strings_replacement_function(snapshot_text)

    open_snapshot_uri = file_path.resolve().as_uri()
    logger.debug(
        LazyFormat(
            "Generated snapshot text",
            snapshot_text=snapshot_text if log_snapshot_text else "<hidden in log output>",
            file_path=file_path,
            open_link=mf_colored_link_text(open_snapshot_uri),
            iterm_hint="Link may be opened with <Command> + <Left Click>",
        )
    )

    # Add a header with context about the snapshot.
    if include_headers:
        path_to_test_file = pathlib.Path(request.node.fspath)
        test_doc_string = request.function.__doc__
        header_lines = [
            f"test_name: {request.node.name}",
            f"test_filename: {path_to_test_file.name}",
        ]
        if test_doc_string is not None:
            header_lines.append("docstring:")
            header_lines.append(mf_indent(test_doc_string.rstrip()))
        if additional_header_fields is not None:
            for header_field_name, header_field_value in additional_header_fields.items():
                header_lines.append(f"{header_field_name}: {header_field_value}")
        if expectation_description is not None:
            header_lines.append(f"{SNAPSHOT_EXPECTATION_DESCRIPTION}:")
            header_lines.append(mf_indent(expectation_description))
        header_lines.append("---")

        snapshot_text = "\n".join(header_lines) + "\n" + snapshot_text

    # Add a new line at the end of the file so that PRs don't show the "no newline" symbol on Github.
    if len(snapshot_text) > 1 and snapshot_text[-1] != "\n":
        snapshot_text = snapshot_text + "\n"

    # If we are in overwrite mode, create / overwrite the snapshot file.:
    if snapshot_configuration.overwrite_snapshots:
        # Create parent directory for the plan text files.
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as snapshot_text_file:
            snapshot_text_file.write(snapshot_text)

    # Throw an exception if the plan is not there.
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"Could not find snapshot file at path {file_path}. Re-run with --overwrite-snapshots and check git status "
            f"to see what's new."
        )

    if snapshot_configuration.display_snapshots:
        if not snapshot_configuration.overwrite_snapshots:
            logger.warning(
                LazyFormat(lambda: f"Not overwriting snapshots, so displaying existing snapshot at {file_path}")
            )

        if len(request.session.items) > 1:
            raise ValueError("Displaying snapshots is only supported when there's a single item in a testing session.")
        webbrowser.open(file_path.resolve().as_uri())

    # Read the existing plan from the file and compare with the actual plan
    with open(file_path, "r") as snapshot_text_file:
        expected_snapshot_text = snapshot_text_file.read()

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
            diff = difflib.unified_diff(
                a=expected_snapshot_text.splitlines(keepends=True),
                b=snapshot_text.splitlines(keepends=True),
                fromfile=f"Expected Result in {file_path}",
                tofile="Actual Result",
            )
            assert False, "Result does not match the stored snapshot. Diff from expected to actual:\n\n" + "".join(diff)


def snapshot_path_prefix(
    request: _pytest.fixtures.FixtureRequest,
    snapshot_configuration: SnapshotConfiguration,
    snapshot_group: str,
    snapshot_id: str,
    additional_sub_directories: Tuple[str, ...] = (),
) -> pathlib.Path:
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

    snapshot_file_name_parts = []
    # Parameterized test names look like 'test_case[some_param]'. "[" and "]" are annoying to deal with in the shell,
    # so replace them with dunders.
    snapshot_file_name_parts.extend(re.split(r"[\[\]]", test_name))
    # A trailing ] will produce an empty string in the list, so remove that.
    snapshot_file_name_parts = [part for part in snapshot_file_name_parts if len(part) > 0]
    snapshot_file_name_parts.append(snapshot_id)

    snapshot_file_name_prefix = "__".join(snapshot_file_name_parts)
    path_to_test_file = pathlib.Path(request.node.fspath)
    directory_to_store_snapshot = snapshot_configuration.snapshot_directory.joinpath(
        snapshot_configuration.snapshot_directory,
        path_to_test_file.name,
        snapshot_group,
    )
    if additional_sub_directories is not None:
        directory_to_store_snapshot = directory_to_store_snapshot.joinpath(*additional_sub_directories)

    return directory_to_store_snapshot.joinpath(snapshot_file_name_prefix)


def _exclude_lines_matching_regex(file_contents: str, exclude_line_regex: str) -> str:
    """Removes lines from file contents if the line matches exclude_regex."""
    compiled_regex = re.compile(exclude_line_regex)
    return "\n".join([line for line in file_contents.split("\n") if not compiled_regex.match(line)])


DISPLAY_SNAPSHOTS_CLI_FLAG = "--display-snapshots"
OVERWRITE_SNAPSHOTS_CLI_FLAG = "--overwrite-snapshots"


def add_display_snapshots_cli_flag(parser: _pytest.config.argparsing.Parser) -> None:  # noqa: D103
    parser.addoption(DISPLAY_SNAPSHOTS_CLI_FLAG, action="store_true", help="Displays snapshots in a browser if set")


def add_overwrite_snapshots_cli_flag(parser: _pytest.config.argparsing.Parser) -> None:  # noqa: D103
    parser.addoption(
        OVERWRITE_SNAPSHOTS_CLI_FLAG,
        action="store_true",
        help="Overwrites existing snapshots by ones generated during this testing session",
    )


# In plan outputs, replace strings that vary from run to run with this so that comparisons can be made
# consistently.
PLACEHOLDER_CHAR_FOR_INCOMPARABLE_STRINGS = "*"


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
    snapshot_configuration: SnapshotConfiguration,
    plan: PlanT,
    plan_snapshot_text: str,
    plan_snapshot_file_extension: str = ".xml",
    exclude_line_regex: Optional[str] = None,
    incomparable_strings_replacement_function: Optional[Callable[[str], str]] = None,
    additional_sub_directories_for_snapshots: Tuple[str, ...] = (),
    expectation_description: Optional[str] = None,
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
        snapshot_configuration=snapshot_configuration,
        group_id=plan.__class__.__name__,
        snapshot_id=str(plan.dag_id),
        snapshot_text=plan_snapshot_text,
        snapshot_file_extension=plan_snapshot_file_extension,
        exclude_line_regex=exclude_line_regex,
        incomparable_strings_replacement_function=incomparable_strings_replacement_function,
        additional_sub_directories_for_snapshots=additional_sub_directories_for_snapshots,
        expectation_description=expectation_description,
    )


def _convert_linkable_element_set_to_rows(
    linkable_element_set: BaseGroupByItemSet,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for annotated_spec in sorted(
        linkable_element_set.annotated_specs,
        key=lambda annotated_spec_in_lambda: annotated_spec_in_lambda.spec.dunder_name,
    ):
        row_dict: dict[str, str] = {
            "Dunder Name": annotated_spec.spec.dunder_name.ljust(78),
        }
        spec_set = group_spec_by_type(annotated_spec.spec)

        if len(spec_set.group_by_metric_specs) == 0:
            row_dict["Metric-Subquery Entity-Links"] = ""
        elif len(spec_set.group_by_metric_specs) == 1:
            group_by_metric_spec = spec_set.group_by_metric_specs[0]
            row_dict["Metric-Subquery Entity-Links"] = ",".join(
                entity_link.element_name for entity_link in group_by_metric_spec.metric_subquery_entity_links
            )
        else:
            raise RuntimeError(LazyFormat("There should have been at most 1 group-by-metric spec", spec_set=spec_set))
        row_dict["Type"] = annotated_spec.element_type.name.ljust(14)

        row_dict["Properties"] = ",".join(
            sorted(linkable_element_property.name for linkable_element_property in annotated_spec.property_set)
        )
        row_dict["Derived-From Semantic Models"] = ",".join(
            sorted(
                model_reference.semantic_model_name for model_reference in annotated_spec.derived_from_semantic_models
            )
        )
        rows.append(row_dict)

    return rows


def assert_linkable_element_set_snapshot_equal(  # noqa: D103
    request: FixtureRequest,
    snapshot_configuration: SnapshotConfiguration,
    linkable_element_set: BaseGroupByItemSet,
    set_id: str = "result",
    expectation_description: Optional[str] = None,
) -> None:
    rows = _convert_linkable_element_set_to_rows(linkable_element_set)

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=snapshot_configuration,
        snapshot_id=set_id,
        snapshot_str=tabulate.tabulate(
            headers="keys",
            tabular_data=sorted(rows, key=lambda row: tuple(row.values())),  # type: ignore[attr-defined]
        ),
        expectation_description=expectation_description,
    )


def assert_spec_set_snapshot_equal(  # noqa: D103
    request: FixtureRequest,
    snapshot_configuration: SnapshotConfiguration,
    set_id: str,
    spec_set: InstanceSpecSet,
    expectation_description: Optional[str] = None,
) -> None:
    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=snapshot_configuration,
        obj_id=set_id,
        obj=sorted(spec.dunder_name for spec in spec_set.all_specs),
        expectation_description=expectation_description,
    )


def assert_linkable_spec_set_snapshot_equal(  # noqa: D103
    request: FixtureRequest,
    snapshot_configuration: SnapshotConfiguration,
    set_id: str,
    spec_set: LinkableSpecSet,
    expectation_description: Optional[str] = None,
) -> None:
    naming_scheme = ObjectBuilderNamingScheme()
    assert_snapshot_text_equal(
        request=request,
        snapshot_configuration=snapshot_configuration,
        group_id=spec_set.__class__.__name__,
        snapshot_id=set_id,
        snapshot_text=mf_pformat(sorted(naming_scheme.input_str(spec) for spec in spec_set.as_tuple)),
        snapshot_file_extension=".txt",
        additional_sub_directories_for_snapshots=(),
        expectation_description=expectation_description,
    )


def assert_object_snapshot_equal(  # type: ignore[misc]
    request: FixtureRequest,
    snapshot_configuration: SnapshotConfiguration,
    obj: Any,
    obj_id: str = "result",
    expectation_description: Optional[str] = None,
    format_option: Optional[PrettyFormatOption] = None,
) -> None:
    """For tests to compare large objects, this can be used to snapshot a text representation of the object."""
    assert_snapshot_text_equal(
        request=request,
        snapshot_configuration=snapshot_configuration,
        group_id=obj.__class__.__name__,
        snapshot_id=obj_id,
        snapshot_text=mf_pformat(obj, format_option),
        snapshot_file_extension=".txt",
        expectation_description=expectation_description,
    )


def assert_str_snapshot_equal(  # noqa: D103
    request: FixtureRequest,
    snapshot_configuration: SnapshotConfiguration,
    snapshot_str: str,
    snapshot_id: str = "result",
    snapshot_file_extension: str = ".txt",
    expectation_description: Optional[str] = None,
    incomparable_strings_replacement_function: Optional[Callable[[str], str]] = None,
    include_headers: bool = True,
) -> None:
    """Write / compare a string snapshot."""
    assert_snapshot_text_equal(
        request=request,
        snapshot_configuration=snapshot_configuration,
        group_id=snapshot_str.__class__.__name__,
        snapshot_id=snapshot_id,
        snapshot_text=snapshot_str,
        snapshot_file_extension=snapshot_file_extension,
        expectation_description=expectation_description,
        incomparable_strings_replacement_function=incomparable_strings_replacement_function,
        include_headers=include_headers,
    )
