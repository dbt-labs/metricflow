from __future__ import annotations

from typing import Protocol


class FileSlice(Protocol):
    """Provides file slice level context about what something was created from"""

    filename: str
    content: str
    start_line_number: int
    end_line_number: int


class MetadataProtocol(Protocol):
    """Provides file context about what something was created from"""

    repo_file_path: str
    file_slice: FileSlice
