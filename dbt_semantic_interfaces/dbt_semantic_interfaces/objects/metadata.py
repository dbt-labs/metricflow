from __future__ import annotations

from dbt_semantic_interfaces.objects.base import HashableBaseModel


class FileSlice(HashableBaseModel):  # noqa: D
    filename: str
    content: str
    start_line_number: int
    end_line_number: int


class Metadata(HashableBaseModel):  # noqa: D
    repo_file_path: str
    file_slice: FileSlice
