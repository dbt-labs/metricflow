from __future__ import annotations

from typing import Optional, List, Dict, Any

from metricflow.model.objects.common import Element
from metricflow.model.objects.utils import ParseableObject, HashableBaseModel
from metricflow.object_utils import ExtendedEnum
from metricflow.specs import IdentifierReference


class IdentifierType(ExtendedEnum):
    """Defines uniqueness and completeness of identifier"""

    FOREIGN = "foreign"
    PRIMARY = "primary"
    UNIQUE = "unique"

    # for identifiers that are only rendered, not joined on (used when rendering composite identifiers)
    # RENDER_ONLY = "render_only" # DEPRECATED ? we shouldnt need in the new world?


class CompositeSubIdentifier(HashableBaseModel, ParseableObject):
    """CompositeSubIdentifiers either describe or reference the identifiers that comprise a composite identifier"""

    name: Optional[IdentifierReference]
    expr: Optional[str]
    ref: Optional[str]


class Identifier(HashableBaseModel, Element, ParseableObject):
    """Describes a identifier"""

    name: IdentifierReference
    type: IdentifierType
    role: Optional[str]
    entity: Optional[str]
    identifiers: List[CompositeSubIdentifier] = []
    expr: Optional[str] = None

    def __init__(  # type: ignore
        self,
        name: IdentifierReference,
        type: IdentifierType,
        role: Optional[str] = None,
        entity: Optional[str] = None,
        identifiers: List[CompositeSubIdentifier] = [],
        expr: Optional[str] = None,
        **kwargs: Dict[str, Any],  # the parser may instantiate objects with additional fields (eg __parsing_context__)
    ) -> None:
        """Normal pydantic initializer except we set entity to name"""
        super().__init__(
            name=name,
            type=type,
            role=role,
            entity=entity,
            identifiers=identifiers,
            expr=expr,
        )
        if self.entity is None:
            self.entity = self.name.element_name

    @property
    def is_primary_time(self) -> bool:  # noqa: D
        return False

    @property
    def is_composite(self) -> bool:  # noqa: D
        return self.identifiers is not None and len(self.identifiers) > 0
