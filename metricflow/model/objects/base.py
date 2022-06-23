from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

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


ModelObjectT_co = TypeVar("ModelObjectT_co", covariant=True, bound=BaseModel)


class PydanticCustomInputParser(ABC, Generic[ModelObjectT_co]):
    """Implements required"""

    @classmethod
    def __get_validators__(cls):
        """Pydantic magic method for allowing parsing of arbitrary input on parse_obj invocation

        This allow for parsing and validation prior to object initialization. Most classes implementing this
        interface in our model are doing so because the input value from user-supplied YAML will be a string
        representation rather than the structured object type.
        """
        yield cls._from_yaml_value

    @classmethod
    @abstractmethod
    def _from_yaml_value(cls, input: Any) -> ModelObjectT_co:
        """Abstract method for providing object-specific parsing logic"""
        raise NotImplementedError()
