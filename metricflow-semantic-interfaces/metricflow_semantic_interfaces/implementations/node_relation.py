from __future__ import annotations

from typing import Any, Optional

from msi_pydantic_shim import validator
from typing_extensions import override

from metricflow_semantic_interfaces.implementations.base import HashableBaseModel
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.protocols.node_relation import NodeRelation


class PydanticNodeRelation(HashableBaseModel, ProtocolHint[NodeRelation]):
    """Path object to where the data should be."""

    alias: str
    schema_name: str
    database: Optional[str] = None
    relation_name: str = ""

    @override
    def _implements_protocol(self) -> NodeRelation:  # noqa: D102
        return self

    @validator("relation_name", always=True)
    @classmethod
    def __create_default_relation_name(cls, value: Any, values: Any) -> str:  # type: ignore[misc]
        """Dynamically build the dot path for `relation_name`, if not specified."""
        if value:
            # Only build the relation_name if it was not present in config.
            return value

        alias, schema, database = values.get("alias"), values.get("schema_name"), values.get("database")
        if alias is None or schema is None:
            raise ValueError(
                f"Failed to build relation_name because alias and/or schema was None. schema: {schema}, alias: {alias}"
            )

        if database is not None:
            value = f"{database}.{schema}.{alias}"
        else:
            value = f"{schema}.{alias}"
        return value

    @staticmethod
    def from_string(sql_str: str) -> PydanticNodeRelation:  # noqa: D102
        sql_str_split = sql_str.split(".")
        if len(sql_str_split) == 2:
            return PydanticNodeRelation(schema_name=sql_str_split[0], alias=sql_str_split[1])
        elif len(sql_str_split) == 3:
            return PydanticNodeRelation(database=sql_str_split[0], schema_name=sql_str_split[1], alias=sql_str_split[2])
        raise RuntimeError(
            f"Invalid input for a SQL table, expected form '<schema>.<table>' or '<db>.<schema>.<table>' "
            f"but got: {sql_str}"
        )
