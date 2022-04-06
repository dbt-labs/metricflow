from enum import Enum
from typing import Tuple
from dataclasses import dataclass
from metricflow.model.objects.elements.identifier import IdentifierType
from metricflow.model.objects.common import Element


class JoinType(Enum):  # noqa: D
    INNER = "inner"
    LEFT = "left"


@dataclass(frozen=True)
class JoinLink:  # noqa: D
    via_from: Element  # identifier for join (left side "from")
    via_to: Element  # identifier for join (right side "target")
    partitions: Tuple[str, ...]  # partitions for join, tuple for hashability
    join_type: JoinType

    @property
    def is_fanout_join(self) -> bool:  # noqa: D
        return (self.via_from.type, self.via_to.type) in FANOUT_JOIN_TYPES  # type: ignore


FANOUT_JOIN_TYPES = set(
    [
        (IdentifierType.PRIMARY, IdentifierType.FOREIGN),
        (IdentifierType.UNIQUE, IdentifierType.FOREIGN),
        (IdentifierType.FOREIGN, IdentifierType.FOREIGN),
    ]
)

JOIN_TYPE_MAPPING = {
    (IdentifierType.PRIMARY, IdentifierType.PRIMARY): JoinType.LEFT,
    (IdentifierType.PRIMARY, IdentifierType.UNIQUE): JoinType.LEFT,
    # (IdentifierType.PRIMARY, IdentifierType.FOREIGN): JoinType.LEFT,  # fans out
    (IdentifierType.UNIQUE, IdentifierType.PRIMARY): JoinType.LEFT,
    (IdentifierType.UNIQUE, IdentifierType.UNIQUE): JoinType.LEFT,
    # (IdentifierType.UNIQUE, IdentifierType.FOREIGN): JoinType.LEFT,  # fans out
    (IdentifierType.FOREIGN, IdentifierType.PRIMARY): JoinType.LEFT,
    (IdentifierType.FOREIGN, IdentifierType.UNIQUE): JoinType.LEFT,
    # (IdentifierType.FOREIGN, IdentifierType.FOREIGN): JoinType.LEFT,  # fans out
}
