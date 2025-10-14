from __future__ import annotations

from copy import deepcopy
from typing import List, Optional, Union

from msi_pydantic_shim import Field
from typing_extensions import Self, override

from metricflow_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from metricflow_semantic_interfaces.implementations.export import PydanticExport
from metricflow_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilterIntersection,
)
from metricflow_semantic_interfaces.implementations.metadata import PydanticMetadata
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.protocols.saved_query import (
    SavedQuery,
    SavedQueryQueryParams,
)


class PydanticSavedQueryQueryParams(HashableBaseModel, ProtocolHint[SavedQueryQueryParams]):
    """Pydantic implementation of SavedQuery."""

    @override
    def _implements_protocol(self) -> SavedQueryQueryParams:
        return self

    metrics: List[str]
    group_by: List[str] = Field(default_factory=list)
    order_by: List[str] = Field(default_factory=list)
    limit: Optional[int] = None
    where: Optional[PydanticWhereFilterIntersection] = None


class PydanticSavedQuery(
    HashableBaseModel,
    ModelWithMetadataParsing,
    ProtocolHint[SavedQuery],
):
    """Pydantic implementation of SavedQuery."""

    @override
    def _implements_protocol(self) -> SavedQuery:
        return self

    name: str
    query_params: PydanticSavedQueryQueryParams
    description: Optional[str] = None
    metadata: Optional[PydanticMetadata] = None
    label: Optional[str] = None
    exports: List[PydanticExport] = Field(default_factory=list)
    tags: Union[str, List[str]] = Field(
        default_factory=list,
    )

    @classmethod
    def parse_obj(cls, input: HashableBaseModel) -> Self:  # noqa: D102
        data = deepcopy(input)
        if isinstance(data, dict):
            if isinstance(data.get("tags"), str):
                data["tags"] = [data["tags"]]
            if isinstance(data.get("tags"), list):
                data["tags"].sort()
        return super(HashableBaseModel, cls).parse_obj(data)
