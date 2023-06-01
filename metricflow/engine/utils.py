from __future__ import annotations

import datetime as dt
from typing import Optional

from dateutil.parser import parse
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.parsing.dir_to_model import (
    SemanticManifestBuildResult,
    parse_directory_of_yaml_files_to_semantic_manifest,
)

from metricflow.configuration.constants import CONFIG_MODEL_PATH
from metricflow.configuration.yaml_handler import YamlFileHandler
from metricflow.errors.errors import ModelCreationException
from metricflow.sql_clients.common_client import not_empty


def path_to_models(handler: YamlFileHandler) -> str:
    """Given a YamlFileHandler, return the path to the YAML model config files."""
    return not_empty(handler.get_value(CONFIG_MODEL_PATH), CONFIG_MODEL_PATH, handler.url)


def model_build_result_from_config(
    handler: YamlFileHandler, raise_issues_as_exceptions: bool = True
) -> SemanticManifestBuildResult:
    """Given a yaml file, creates a ModelBuildResult.

    Args:
        handler: a file handler for loading the configs from
        raise_issues_as_exceptions: determines if issues should be raised, or returned as issues

    Returns:
        ModelBuildResult that contains the UserConfigureModel and any associated ValidationIssues
    """
    models_path = path_to_models(handler=handler)
    try:
        return parse_directory_of_yaml_files_to_semantic_manifest(
            models_path, raise_issues_as_exceptions=raise_issues_as_exceptions
        )
    except Exception as e:
        raise ModelCreationException from e


def build_semantic_manifest_from_config(handler: YamlFileHandler) -> PydanticSemanticManifest:
    """Given a yaml file, create a SemanticManifest."""
    return model_build_result_from_config(handler=handler).semantic_manifest


def convert_to_datetime(datetime_str: Optional[str]) -> Optional[dt.datetime]:
    """Callback to convert string to datetime given as an iso8601 timestamp."""
    if datetime_str is None:
        return None

    try:
        return parse(datetime_str)
    except Exception:
        raise ValueError(f"'{datetime_str}' is not a valid iso8601 timestamp")
