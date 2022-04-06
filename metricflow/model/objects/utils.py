from __future__ import annotations

import json
from abc import ABC, abstractmethod

from pydantic import BaseModel


class HashableBaseModel(BaseModel):
    """Extends BaseModel with a generic hash function"""

    def __hash__(self) -> int:  # noqa: D
        return hash(json.dumps(self.json(sort_keys=True), sort_keys=True))


class FrozenBaseModel(HashableBaseModel):
    """Similar to HashableBaseModel but faux immutable."""

    class Config:
        """Pydantic feature."""

        allow_mutation = False

    def to_pretty_json(self) -> str:
        """Convert to a pretty JSON representation."""
        raw_json_str = self.json()
        json_obj = json.loads(raw_json_str)
        return json.dumps(json_obj, indent=4)

    def __str__(self) -> str:  # noqa: D
        return self.__repr__()


class ParseableObject:  # noqa: D
    pass


class ParseableField(ABC):  # noqa: D
    @staticmethod
    @abstractmethod
    def parse(s: str) -> ParseableField:  # noqa: D
        pass
