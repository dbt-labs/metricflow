from __future__ import annotations

from metricflow_semantic_interfaces.errors import InvalidQuerySyntax
from metricflow_semantic_interfaces.parsing.text_input.ti_description import (
    ObjectBuilderMethod,
    QueryItemType,
)


class QueryItemJinjaException(Exception):
    """Raised when there is an exception when calling Jinja package methods on the query item input."""

    pass


class InvalidBuilderMethodException(InvalidQuerySyntax):
    """Raised when a query item using the object-builder format uses a disallowed method.

    For example, `Entity('listing').grain('day')` should raise this exception since `grain` is only applicable to
    `Dimension()`.
    """

    def __init__(  # noqa: D107
        self, message: str, item_type: QueryItemType, invalid_builder_method: ObjectBuilderMethod
    ) -> None:
        super().__init__(message)
        self._item_type = item_type
        self._invalid_builder_method = invalid_builder_method

    @property
    def item_type(self) -> QueryItemType:
        """Return the item that was used with the invalid method."""
        return self._item_type

    @property
    def invalid_builder_method(self) -> ObjectBuilderMethod:
        """Return the invalid builder method that was used."""
        return self._invalid_builder_method
