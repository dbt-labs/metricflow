from __future__ import annotations

import logging
import pathlib
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypeVar

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.helpers.string_helpers import mf_dedent
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_print import mf_pformat, mf_pformat_dict
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import SnapshotConfiguration, assert_str_snapshot_equal
from tests_metricflow_semantics.experimental.mf_graph.flow_graph import FlowGraph
from tests_metricflow_semantics.experimental.mf_graph.formatting.svg_formatter import SvgFormatter

logger = logging.getLogger(__name__)


def test_svg_text(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, flow_graph: FlowGraph
) -> None:
    """Check formatting of the graph to an SVG."""
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=flow_graph.format(SvgFormatter()),
    )
