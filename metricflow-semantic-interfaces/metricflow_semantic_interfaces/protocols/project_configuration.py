from __future__ import annotations

from abc import abstractmethod
from typing import Protocol, Sequence

from metricflow_semantic_interfaces.protocols.semantic_version import SemanticVersion
from metricflow_semantic_interfaces.protocols.time_spine import TimeSpine
from metricflow_semantic_interfaces.protocols.time_spine_configuration import (
    TimeSpineTableConfiguration,
)


class ProjectConfiguration(Protocol):
    """Configuration options for the project associated with a semantic manifest."""

    @property
    @abstractmethod
    def dsi_package_version(self) -> SemanticVersion:
        """Version of the dbt-semantic-interfaces package used to define this manifest."""
        pass

    @property
    @abstractmethod
    def time_spines(self) -> Sequence[TimeSpine]:
        """The time spine table configurations. Multiple allowed for different time grains."""
        pass

    @property
    @abstractmethod
    def time_spine_table_configurations(self) -> Sequence[TimeSpineTableConfiguration]:
        """Legacy time spine table configurations. In the process of deprecation."""
        pass
