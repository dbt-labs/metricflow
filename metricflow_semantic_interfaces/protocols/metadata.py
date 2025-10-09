from __future__ import annotations

from abc import abstractmethod
from typing import Protocol


class FileSlice(Protocol):
    """Provides file slice level context about what something was created from."""

    @property
    @abstractmethod
    def filename(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def content(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def start_line_number(self) -> int:  # noqa: D
        pass

    @property
    @abstractmethod
    def end_line_number(self) -> int:  # noqa: D
        pass


class Metadata(Protocol):
    """Provides file context about what something was created from."""

    @property
    @abstractmethod
    def repo_file_path(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def file_slice(self) -> FileSlice:  # noqa: D
        pass
