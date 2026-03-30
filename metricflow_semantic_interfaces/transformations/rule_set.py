from __future__ import annotations

import logging
from abc import abstractmethod
from typing import Protocol, Sequence

from metricflow_semantic_interfaces.protocols import SemanticManifestT
from metricflow_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)

logger = logging.getLogger(__name__)


class SemanticManifestTransformRuleSet(Protocol[SemanticManifestT]):
    """Groups rules that should be run for a SemanticManifest."""

    @property
    @abstractmethod
    def primary_rules(self) -> Sequence[SemanticManifestTransformRule[SemanticManifestT]]:
        """TODO: Define what primary means."""
        pass

    @property
    @abstractmethod
    def secondary_rules(self) -> Sequence[SemanticManifestTransformRule[SemanticManifestT]]:
        """TODO: Define what secondary means."""
        pass

    @property
    @abstractmethod
    def all_rules(self) -> Sequence[Sequence[SemanticManifestTransformRule[SemanticManifestT]]]:
        """TODO: Why a nested sequence?"""
        pass
