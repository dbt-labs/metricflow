from __future__ import annotations

import os
from typing import Dict, Optional

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.parsing.dir_to_model import (
    SemanticManifestBuildResult,
    parse_directory_of_yaml_files_to_semantic_manifest,
)
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator


def load_semantic_manifest(
    relative_manifest_path: str,
    template_mapping: Optional[Dict[str, str]] = None,
) -> SemanticManifestBuildResult:
    """Reads the manifest YAMLs from the standard location, applies transformations, runs validations."""
    yaml_file_directory = os.path.join(os.path.dirname(__file__), f"semantic_manifest_yamls/{relative_manifest_path}")
    build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        yaml_file_directory, template_mapping=template_mapping
    )
    validator = SemanticManifestValidator[PydanticSemanticManifest]()
    validator.checked_validations(build_result.semantic_manifest)
    return build_result
