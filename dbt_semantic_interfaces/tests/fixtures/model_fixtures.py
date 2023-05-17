from __future__ import annotations

import datetime
import logging
import os
from typing import Dict
import uuid

import pytest

from dbt_semantic_interfaces.model_transformer import ModelTransformer
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.parsing.dir_to_model import parse_directory_of_yaml_files_to_model

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def template_mapping() -> Dict[str, str]:
    """Mapping for template variables in the model YAML files."""
    current_time = datetime.datetime.now().strftime("%Y_%m_%d")
    random_suffix = uuid.uuid4()
    system_schema = f"mf_test_{current_time}_{random_suffix}"
    return {"source_schema": system_schema}


@pytest.fixture(scope="session")
def simple_semantic_manifest(template_mapping: Dict[str, str]) -> SemanticManifest:
    """Model used for many tests."""

    model_build_result = parse_directory_of_yaml_files_to_model(
        os.path.join(os.path.dirname(__file__), "model_yamls/simple_model"), template_mapping=template_mapping
    )
    return model_build_result.model


@pytest.fixture(scope="session")
def simple_model__with_primary_transforms(template_mapping: Dict[str, str]) -> SemanticManifest:
    """Model used for tests pre-transformations."""

    model_build_result = parse_directory_of_yaml_files_to_model(
        os.path.join(os.path.dirname(__file__), "model_yamls/simple_model"),
        template_mapping=template_mapping,
        apply_transformations=False,
    )
    transformed_model = ModelTransformer.transform(
        model=model_build_result.model, ordered_rule_sequences=(ModelTransformer.PRIMARY_RULES,)
    )
    return transformed_model
