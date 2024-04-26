from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from dbt_semantic_interfaces.references import SemanticModelReference


class SemanticModelDerivation(ABC):
    """Interface for an object that can be described as derived from a semantic model."""

    @property
    @abstractmethod
    def derived_from_semantic_models(self) -> Sequence[SemanticModelReference]:
        """The semantic models that this was derived from.

        The returned sequence should be ordered and not contain duplicates.
        """
        raise NotImplementedError
