from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from dbt_semantic_interfaces.references import SemanticModelReference


class SemanticModelDerivation(ABC):
    """Interface for an object that can be described as derived from a semantic model."""

    # There are some cases where we create virtual elements that don't directly correspond to something in the manifest.
    # For example, when querying `metric_time` without any metrics. To avoid having an `Optional` field to handle those
    # cases, we're trying out a case where we use this reference.
    VIRTUAL_SEMANTIC_MODEL_REFERENCE = SemanticModelReference("__VIRTUAL__")

    @property
    @abstractmethod
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        """The semantic models that this was derived from.

        The returned sequence should be ordered and not contain duplicates.
        """
        raise NotImplementedError
