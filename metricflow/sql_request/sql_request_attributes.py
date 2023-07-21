from __future__ import annotations

import logging
import typing
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from operator import itemgetter
from typing import Any, Dict, Optional, Sequence

from dbt_semantic_interfaces.implementations.base import FrozenBaseModel
from pydantic import Field

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SqlRequestId:
    """Identifies a request (i.e. a call to SqlClient.query() or SqlClient.execute()) to the SQL engine."""

    id_str: str

    def __repr__(self) -> str:  # noqa: D
        return self.id_str


class SqlRequestTagSet(FrozenBaseModel):
    """Set of tags as a Pydantic model for easy serialization."""

    # Using strings to make for cleaner serialized output. Clients should not use this field directly.
    tag_dict: typing.OrderedDict[str, str] = Field(default_factory=OrderedDict)

    @property
    def tags(self) -> Sequence[SqlRequestTag]:  # noqa: D
        return tuple(SqlRequestTag(key, value) for key, value in self.tag_dict.items())

    @staticmethod
    def create_from_dict(tag_dict: Dict[SqlRequestTagKey, str]) -> SqlRequestTagSet:  # noqa: D
        str_tag_dict = {tag_key_enum.value: value for tag_key_enum, value in tag_dict.items()}
        sorted_tuples = tuple(sorted(str_tag_dict.items(), key=itemgetter(0, 1)))
        return SqlRequestTagSet(tag_dict=OrderedDict(sorted_tuples))

    @staticmethod
    def create_from_request_id(request_id: SqlRequestId) -> SqlRequestTagSet:
        """Create a tag set that only includes the tag for the request ID."""
        tag_dict = OrderedDict()
        tag_dict[SqlRequestTagKey.REQUEST_ID_KEY.value] = request_id.id_str
        return SqlRequestTagSet(tag_dict=tag_dict)

    def add_request_id(self, request_id: SqlRequestId) -> SqlRequestTagSet:
        """Adds the request ID tag to this set."""
        tag_dict = OrderedDict()
        tag_dict[SqlRequestTagKey.REQUEST_ID_KEY.value] = request_id.id_str
        return SqlRequestTagSet.combine((self, SqlRequestTagSet(tag_dict=tag_dict)))

    @staticmethod
    def combine(tag_sets: Sequence[SqlRequestTagSet]) -> SqlRequestTagSet:  # noqa: D
        tag_dict: OrderedDict[str, str] = OrderedDict()
        for tag_set in tag_sets:
            for key, value in tag_set.tag_dict.items():
                if key in tag_dict and tag_dict[key] != value:
                    raise RuntimeError(
                        f"Can't combine tag sets due to a conflicting value for key: {key}. Conflicting values are "
                        f"at least: {value} and {tag_dict[key]}"
                    )
                tag_dict[key] = value

        return SqlRequestTagSet(tag_dict=tag_dict)

    @property
    def request_id(self) -> Optional[SqlRequestId]:
        """The value of the request ID tag."""
        tag_value = self.tag_dict.get(SqlRequestTagKey.REQUEST_ID_KEY.value)
        if tag_value:
            return SqlRequestId(tag_value)
        return None

    def is_subset_of(self, tag_set: SqlRequestTagSet) -> bool:  # noqa: D
        return self.tag_dict.items() <= tag_set.tag_dict.items()


@dataclass(frozen=True)
class SqlRequestTag:
    """A key / value that can be used ot label requests to the SQL engine."""

    key: str
    value: str


class SqlRequestTagKey(Enum):
    """Specific tags used by the system."""

    REQUEST_ID_KEY = "MF_REQUEST_ID"


MF_SYSTEM_TAGS_KEY = "MF_SYSTEM_TAGS"
MF_EXTRA_TAGS_KEY = "MF_EXTRA_TAGS"

# Helps to reduce the need too have "ignore type" everywhere.
JsonDict = Dict[str, Any]  # type: ignore [misc]


class SqlJsonTag:
    """Immutable object that represents a JSON object to be used for tagging SQL requests."""

    def __init__(self, json_dict: Optional[JsonDict] = None) -> None:  # noqa: D
        self._json_dict = OrderedDict(json_dict or {})

    @property
    def json_dict(self) -> JsonDict:  # noqa: D
        return OrderedDict(self._json_dict)

    def combine(self, other_tag: SqlJsonTag) -> SqlJsonTag:  # noqa: D
        new_json_dict = OrderedDict(self._json_dict)
        for k, v in other_tag._json_dict.items():
            if k in new_json_dict:
                logger.error(
                    f"Conflict while combining tags. Conflict key: {k} Conflicting values: {v} and {new_json_dict[k]}"
                )
            new_json_dict[k] = v
        return SqlJsonTag(new_json_dict)

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(json_dict={self._json_dict})"
