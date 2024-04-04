from __future__ import annotations

import datetime
import difflib
import logging
import os
import re
import webbrowser
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

import _pytest
import _pytest.fixtures

from metricflow.time.time_source import TimeSource

logger = logging.getLogger(__name__)

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


@dataclass(frozen=True)
class SnapshotConfiguration:
    """Configuration for handling snapshots in a test session."""

    # Whether to display the snapshot associated with a test session in a browser window.
    display_snapshots: bool
    # Whether to overwrite any text files that were generated.
    overwrite_snapshots: bool


@dataclass(frozen=True)
class MetricFlowTestConfiguration(SnapshotConfiguration):
    """State that is shared between tests during a testing session."""

    sql_engine_url: str
    sql_engine_password: str
    # Where MF system tables should be stored.
    mf_system_schema: str
    # Where tables for test data sets should be stored.
    mf_source_schema: str

    # Whether to display the graph associated with a test session in a browser window.
    display_graphs: bool

    # The source schema contains tables that are used for running tests. If this is set, a source schema in the SQL
    # is created and persisted between runs. The source schema name includes a hash of the tables that should be in
    # the schema, so
    use_persistent_source_schema: bool


def assert_snapshot_text_equal(
    request: _pytest.fixtures.FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
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

    # Add a new line at the end of the file so that PRs don't show the "no newline" symbol on Github.
    if len(snapshot_text) > 1 and snapshot_text[-1] != "\n":
        snapshot_text = snapshot_text + "\n"

    # If we are in overwrite mode, make a new plan:
    if mf_test_configuration.overwrite_snapshots:
        # Create parent directory for the plan text files.
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as snapshot_text_file:
            snapshot_text_file.write(snapshot_text)

    # Throw an exception if the plan is not there.
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"Could not find snapshot file at path {file_path}. Re-run with --overwrite-snapshots and check git status "
            "to see what's new."
        )

    if mf_test_configuration.display_snapshots:
        if not mf_test_configuration.overwrite_snapshots:
            logger.warning(f"Not overwriting snapshots, so displaying existing snapshot at {file_path}")

        if len(request.session.items) > 1:
            raise ValueError("Displaying snapshots is only supported when there's a single item in a testing session.")
        webbrowser.open("file://" + file_path)

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
            differ = difflib.Differ()
            diff = differ.compare(expected_snapshot_text.splitlines(), snapshot_text.splitlines())
            assert False, f"Snapshot from {file_path} does not match. Diff from expected to actual:\n" + "\n".join(diff)


def snapshot_path_prefix(
    request: _pytest.fixtures.FixtureRequest,
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

    snapshot_file_name_parts = []
    # Parameterized test names look like 'test_case[some_param]'. "[" and "]" are annoying to deal with in the shell,
    # so replace them with dunders.
    snapshot_file_name_parts.extend(re.split(r"[\[\]]", test_name))
    # A trailing ] will produce an empty string in the list, so remove that.
    snapshot_file_name_parts = [part for part in snapshot_file_name_parts if len(part) > 0]
    snapshot_file_name_parts.append(snapshot_id)

    snapshot_file_name = "__".join(snapshot_file_name_parts)

    path_items: List[str] = []

    test_file_path_items = os.path.normpath(request.node.fspath).split(os.sep)
    test_file_name = test_file_path_items[-1]
    # Default to where this is defined, but use more appropriate directories if found.
    test_directory_root_index = -1
    for i, path_item in enumerate(test_file_path_items):
        if path_item in ("tests", "metricflow"):
            test_directory_root_index = i + 1

    path_to_store_snapshots = os.sep.join(test_file_path_items[:test_directory_root_index])
    path_items.extend([path_to_store_snapshots, "snapshots", test_file_name, snapshot_group])

    if additional_sub_directories:
        path_items.extend(additional_sub_directories)
    path_items.append(snapshot_file_name)

    return os.path.abspath(os.path.join(*path_items))


def _exclude_lines_matching_regex(file_contents: str, exclude_line_regex: str) -> str:
    """Removes lines from file contents if the line matches exclude_regex."""
    compiled_regex = re.compile(exclude_line_regex)
    return "\n".join([line for line in file_contents.split("\n") if not compiled_regex.match(line)])


class ConfigurableTimeSource(TimeSource):
    """A time source that can be configured so that scheduled operations can be simulated in testing."""

    def __init__(self, configured_time: datetime.datetime) -> None:  # noqa: D107
        self._configured_time = configured_time

    def get_time(self) -> datetime.datetime:  # noqa: D102
        return self._configured_time

    def set_time(self, new_time: datetime.datetime) -> datetime.datetime:  # noqa: D102
        self._configured_time = new_time
        return new_time
