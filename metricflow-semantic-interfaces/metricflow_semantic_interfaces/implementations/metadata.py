from __future__ import annotations

from metricflow_semantic_interfaces.implementations.base import HashableBaseModel


class PydanticFileSlice(HashableBaseModel):  # noqa: D101
    filename: str
    content: str
    start_line_number: int
    end_line_number: int


class PydanticMetadata(HashableBaseModel):  # noqa: D101
    repo_file_path: str
    file_slice: PydanticFileSlice
