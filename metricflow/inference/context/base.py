from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Dict, Type


class InferenceContext(ABC):
    """Encapsulates information that can be used by an inference signaling rule or inference policy."""

    # A registry for keeping track of all known InferenceContext types.
    # This is useful for magicking them into existance from type annotation strings.
    # It maps class names to class types.
    known_subclasses: Dict[str, Type[InferenceContext]] = {}

    def __init_subclass__(cls) -> None:
        """Adds all `InferenceContext` subclasses to the `known_subclasses` registry."""
        InferenceContext.known_subclasses[cls.__name__] = cls

        super().__init_subclass__()


TContext = TypeVar("TContext", bound=InferenceContext)


class InferenceContextProvider(Generic[TContext], ABC):
    """Provides a populated inference context from some datasource."""

    @abstractmethod
    def get_context(self) -> TContext:
        """Fetch inference context data and return it."""
        pass
