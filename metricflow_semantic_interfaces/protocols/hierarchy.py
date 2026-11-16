from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol, Sequence


class DimensionHierarchyLevel(Protocol):
    """A level in a dimension hierarchy.

    `dimension` is a group-by name as seen from the semantic model that defines the hierarchy: a bare dimension name
    (`city`) for a dimension of that semantic model, or a name qualified by entity links (`store__region`) for a
    dimension reached by joining through those entities.
    """

    @property
    @abstractmethod
    def dimension(self) -> str:  # noqa: D102
        pass

    @property
    @abstractmethod
    def parent(self) -> Optional[str]:
        """The dimension holding the parent value of each value at this level, for a self-referencing level.

        For example, a level `employee` with `parent: manager` nests employees under their managers, to any depth.
        The parent is reached through the same entity links as `dimension`, so both values come from the same row.
        """
        pass


class DimensionHierarchy(Protocol):
    """An ordered path of dimensions from the coarsest level to the finest, e.g. country -> state -> city.

    A dimension may appear in more than one hierarchy.
    """

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D102
        pass

    @property
    @abstractmethod
    def label(self) -> Optional[str]:
        """A human readable name for the hierarchy, which, unlike `name`, is not limited to identifier characters."""
        pass

    @property
    @abstractmethod
    def levels(self) -> Sequence[DimensionHierarchyLevel]:
        """The levels of this hierarchy, ordered from the coarsest to the finest."""
        pass
