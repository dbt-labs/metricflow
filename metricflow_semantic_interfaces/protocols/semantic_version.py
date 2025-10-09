from abc import abstractmethod
from typing import Optional, Protocol


class SemanticVersion(Protocol):
    """Represents a semantic version in the MAJOR.MINOR.PATCH format."""

    @property
    @abstractmethod
    def major_version(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def minor_version(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def patch_version(self) -> Optional[str]:  # noqa: D
        pass
