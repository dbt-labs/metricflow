from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow_semantic_interfaces.protocols import SemanticManifest


class MetricFlowRequestGenerator(ABC):
    """ABC for a class to generate requests for testing."""

    @abstractmethod
    def generate_requests(self, semantic_manifest: SemanticManifest) -> Sequence[MetricFlowQueryRequest]:  # noqa: D102
        raise NotImplementedError
