from __future__ import annotations

from typing import Dict, Optional

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.parsing.dir_to_model import (
    SemanticManifestBuildResult,
    parse_directory_of_yaml_files_to_semantic_manifest,
)
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator

from metricflow_semantics.test_helpers.semantic_manifest_yamls import SEMANTIC_MANIFEST_YAMLS_PATH_ANCHOR


def load_semantic_manifest(
    relative_manifest_path: str,
    template_mapping: Optional[Dict[str, str]] = None,
) -> SemanticManifestBuildResult:
    """Reads the manifest YAMLs from the standard location, applies transformations, runs validations."""
    yaml_file_directory = SEMANTIC_MANIFEST_YAMLS_PATH_ANCHOR.directory.joinpath(relative_manifest_path)
    build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        str(yaml_file_directory), template_mapping=template_mapping
    )
    validator = SemanticManifestValidator[PydanticSemanticManifest]()
    validator.checked_validations(build_result.semantic_manifest)
    return build_result


def load_named_manifest(template_mapping: Dict[str, str], manifest_name: str) -> PydanticSemanticManifest:  # noqa: D103
    try:
        build_result = load_semantic_manifest(manifest_name, template_mapping)
        return build_result.semantic_manifest
    except Exception as e:
        raise RuntimeError(f"Error while loading semantic manifest: {manifest_name}") from e
