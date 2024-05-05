from __future__ import annotations

import pathlib
from typing import Dict, Optional

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.parsing.dir_to_model import (
    parse_directory_of_yaml_files_to_semantic_manifest,
)
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator


def load_semantic_manifest(
    yaml_file_directory: pathlib.Path,
    template_mapping: Optional[Dict[str, str]] = None,
) -> PydanticSemanticManifest:
    """Reads the manifest YAMLs from the standard location, applies transformations, runs validations."""
    try:
        build_result = parse_directory_of_yaml_files_to_semantic_manifest(
            str(yaml_file_directory), template_mapping=template_mapping
        )
        validator = SemanticManifestValidator[PydanticSemanticManifest]()
        validator.checked_validations(build_result.semantic_manifest)
        return build_result.semantic_manifest
    except Exception as e:
        raise RuntimeError(f"Error while loading semantic manifest: {yaml_file_directory}") from e
