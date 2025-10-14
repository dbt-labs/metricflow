from __future__ import annotations

from abc import abstractmethod
from typing import Protocol, Sequence, TypeVar

from metricflow_semantic_interfaces.protocols.metric import Metric
from metricflow_semantic_interfaces.protocols.project_configuration import ProjectConfiguration
from metricflow_semantic_interfaces.protocols.saved_query import SavedQuery
from metricflow_semantic_interfaces.protocols.semantic_model import SemanticModel


class SemanticManifest(Protocol):
    """Semantic Manifest holds all the information a SemanticLayer needs to render a query."""

    @property
    @abstractmethod
    def semantic_models(self) -> Sequence[SemanticModel]:  # noqa: D102
        pass

    @property
    @abstractmethod
    def metrics(self) -> Sequence[Metric]:  # noqa: D102
        pass

    @property
    @abstractmethod
    def project_configuration(self) -> ProjectConfiguration:  # noqa: D102
        pass

    @property
    def saved_queries(self) -> Sequence[SavedQuery]:  # noqa: D102
        pass


SemanticManifestT = TypeVar("SemanticManifestT", bound=SemanticManifest)
