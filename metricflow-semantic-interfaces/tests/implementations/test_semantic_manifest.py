from __future__ import annotations

from importlib_metadata import version
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_version import (
    PydanticSemanticVersion,
)

from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION


def test_interfaces_version_matches() -> None:
    """Test that the interfaces_version property returns the installed version of dbt_semantic_interfaces."""
    semantic_manifest = PydanticSemanticManifest(
        semantic_models=[],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    # get the actual installed version
    installed_version = version("dbt_semantic_interfaces")
    assert semantic_manifest.project_configuration.dsi_package_version == PydanticSemanticVersion.create_from_string(
        installed_version
    )
