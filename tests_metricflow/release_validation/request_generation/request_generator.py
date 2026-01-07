from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from dbt_semantic_interfaces.protocols import SemanticManifest

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest


class MetricFlowRequestGenerator(ABC):
    """ABC for a class to generate requests for testing."""

    @abstractmethod
    def generate_requests(self, semantic_manifest: SemanticManifest) -> Sequence[MetricFlowQueryRequest]:  # noqa: D102
        raise NotImplementedError
