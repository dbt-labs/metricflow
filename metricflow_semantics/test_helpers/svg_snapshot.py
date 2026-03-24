from __future__ import annotations

import logging
import os

import _pytest.fixtures

from metricflow_semantics.test_helpers.snapshot_helpers import SnapshotConfiguration, snapshot_path_prefix
from metricflow_semantics.test_helpers.terminal_helpers import mf_colored_link_text
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


def write_svg_snapshot_for_review(
    request: _pytest.fixtures.FixtureRequest,
    snapshot_configuration: SnapshotConfiguration,
    svg_file_contents: str,
) -> None:
    """Write a snapshot of the SVG file contents to aid review.

    * In a Github PR, SVG files are displayed as images.
    * The snapshot is not compared as SVG rendering via `graphviz` is not deterministic (e.g. between platforms).
    * These snapshots should be deleted before merging. Considering adding Github actions to help with this.
    """
    file_path = snapshot_path_prefix(
        request=request,
        snapshot_configuration=snapshot_configuration,
        snapshot_group="svg",
        snapshot_id="result",
    ).with_suffix(".svg")

    if snapshot_configuration.overwrite_snapshots:
        # Create parent directory for the plan text files.
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as snapshot_text_file:
            snapshot_text_file.write(svg_file_contents)
        logger.debug(
            LazyFormat(
                "Wrote SVG for PR review",
                file_path=file_path,
                open_link=mf_colored_link_text(file_path.resolve().as_uri()),
                iterm_hint="Link may be opened with <Command> + <Left Click>",
            )
        )
    else:
        logger.debug(
            LazyFormat(
                "Since this session is not overwriting snapshots, leaving snapshot file alone", file_path=file_path
            )
        )
