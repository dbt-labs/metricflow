from __future__ import annotations

from pydantic import validator
from typing import Any, Optional, List

from metricflow.model.objects.base import HashableBaseModel, ModelWithMetadataParsing
from metricflow.model.objects.common import Metadata
from metricflow.object_utils import ExtendedEnum
from metricflow.references import IdentifierReference, CompositeSubIdentifierReference


class IdentifierType(ExtendedEnum):
    """Defines uniqueness and completeness of identifier"""

    FOREIGN = "foreign"
    PRIMARY = "primary"
    UNIQUE = "unique"

    # for identifiers that are only rendered, not joined on (used when rendering composite identifiers)
    # RENDER_ONLY = "render_only" # DEPRECATED ? we shouldnt need in the new world?


class CompositeSubIdentifier(HashableBaseModel):
    """CompositeSubIdentifiers either describe or reference the identifiers that comprise a composite identifier"""

    name: Optional[str]
    expr: Optional[str]
    ref: Optional[str]

    @property
    def reference(self) -> CompositeSubIdentifierReference:  # noqa: D
        assert self.name, f"The element name should have been set during model transformation. Got {self}"
        return CompositeSubIdentifierReference(element_name=self.name)


class Identifier(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a identifier"""

    name: str
    description: Optional[str]
    type: IdentifierType
    role: Optional[str]
    entity: Optional[str]
    identifiers: List[CompositeSubIdentifier] = []
    expr: Optional[str] = None
    metadata: Optional[Metadata]

    @validator("entity", always=True)
    @classmethod
    def default_entity_value(cls, value: Any, values: Any) -> str:  # type: ignore[misc]
        """Defaulting the value of the identifier 'entity' value using pydantic validator

        If an entity value is provided that is a string, that will become the value of
        entity. If the provifed entity value is None, the entity value becomes the
        element_name representation of the identifier's name.
        """

        if value is None:
            if "name" not in values:
                raise ValueError("Failed to default entity value because objects name value was not defined")
            value = values["name"]

        # guarantee value is string
        if not isinstance(value, str):
            raise ValueError(f"Entity value should be a string (str) type, but got {type(value)} with value: {value}")
        return value

    @property
    def is_primary_time(self) -> bool:  # noqa: D
        return False

    @property
    def is_composite(self) -> bool:  # noqa: D
        return self.identifiers is not None and len(self.identifiers) > 0

    @property
    def reference(self) -> IdentifierReference:  # noqa: D
        return IdentifierReference(element_name=self.name)
