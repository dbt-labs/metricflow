from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from metricflow_semantics.toolkit.mf_graph.path_finding.graph_path import MutableGraphPath
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext
from typing_extensions import override

from metricflow.metric_evaluation.plan.me_edges import MetricQueryDependencyEdge
from metricflow.metric_evaluation.plan.me_nodes import MetricQueryNode

logger = logging.getLogger(__name__)


@dataclass
class MutableMetricEvaluationPath(MutableGraphPath[MetricQueryNode, MetricQueryDependencyEdge]):
    """A mutable path in the metric evaluation graph."""

    @staticmethod
    def create(start_node: Optional[MetricQueryNode] = None) -> MutableMetricEvaluationPath:
        """Create an empty path, optionally initialized with the given start node."""
        return MutableMetricEvaluationPath(
            _nodes=[start_node] if start_node is not None else [],
            _edges=[],
            _weight_addition_order=[],
            _current_weight=0,
            _current_node_set=set(),
            _node_set_addition_order=[],
        )

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format(self._nodes)
