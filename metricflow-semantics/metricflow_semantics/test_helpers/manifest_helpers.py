from __future__ import annotations

import json
import pathlib
from pathlib import Path
from typing import Dict, Optional

from dbt_semantic_interfaces.implementations.metric import PydanticMetric
from dbt_semantic_interfaces.implementations.project_configuration import PydanticProjectConfiguration
from dbt_semantic_interfaces.implementations.saved_query import PydanticSavedQuery
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.parsing.dir_to_model import (
    parse_directory_of_yaml_files_to_semantic_manifest,
)
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat


def mf_load_manifest_from_yaml_directory(
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
        raise RuntimeError(
            LazyFormat("Error while loading semantic manifest", yaml_file_directory=yaml_file_directory)
        ) from e


def mf_load_manifest_from_json_file(
    json_file_path: Path, override_project_configuration: Optional[PydanticProjectConfiguration] = None
) -> PydanticSemanticManifest:
    """Load a manifest from a file containing the JSON-serialized form of a `PydanticSemanticManifest`.

    A project configuration can be provided to override the one in the manifest.
    """
    try:
        with open(json_file_path) as fp:
            manifest_json = json.load(fp)

        semantic_models = [
            PydanticSemanticModel.parse_obj(semantic_model_json)
            for semantic_model_json in manifest_json["semantic_models"]
        ]
        metrics = [PydanticMetric.parse_obj(metric_json) for metric_json in manifest_json["metrics"]]
        saved_queries = [
            PydanticSavedQuery.parse_obj(saved_query_json) for saved_query_json in manifest_json["saved_queries"]
        ]
        project_configuration = override_project_configuration or PydanticProjectConfiguration.parse_obj(
            manifest_json["project_configuration"]
        )

        return PydanticSemanticManifest(
            semantic_models=semantic_models,
            metrics=metrics,
            saved_queries=saved_queries,
            project_configuration=project_configuration,
        )

    except Exception as e:
        raise RuntimeError(LazyFormat("Error while loading semantic manifest", json_file_path=json_file_path)) from e
