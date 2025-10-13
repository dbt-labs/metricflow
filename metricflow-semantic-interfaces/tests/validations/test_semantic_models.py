from __future__ import annotations

import textwrap

import pytest
from metricflow_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.parsing.dir_to_model import (
    parse_yaml_files_to_validation_ready_semantic_manifest,
)
from metricflow_semantic_interfaces.parsing.objects import YamlConfigFile
from metricflow_semantic_interfaces.test_utils import semantic_model_with_guaranteed_meta
from metricflow_semantic_interfaces.type_enums import DimensionType, TimeGranularity
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from metricflow_semantic_interfaces.validations.semantic_models import (
    SemanticModelDefaultsRule,
)
from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)

from tests.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)


@pytest.mark.skip("TODO: Will convert to validation rule")
def test_semantic_model_invalid_sql() -> None:  # noqa: D103
    with pytest.raises(SemanticManifestValidationException, match=r"Invalid SQL"):
        semantic_model_with_guaranteed_meta(
            name="invalid_sql_source",
            dimensions=[
                PydanticDimension(
                    name="ds",
                    type=DimensionType.TIME,
                    type_params=PydanticDimensionTypeParams(
                        time_granularity=TimeGranularity.DAY,
                    ),
                )
            ],
        )


def test_semantic_model_defaults_invalid() -> None:  # noqa: D103
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: semantic_model_name
          node_relation:
            schema_name: some_schema
            alias: some_alias
          defaults:
            agg_time_dimension: doesnotexist
          dimensions:
            - name: ds
              type: time
              type_params:
                time_granularity: day
        """
    )
    invalid_defaults_file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    model = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, invalid_defaults_file]
    )

    with pytest.raises(
        SemanticManifestValidationException, match="'doesnotexist' which doesn't exist as a time dimension"
    ):
        SemanticManifestValidator[PydanticSemanticManifest]([SemanticModelDefaultsRule()]).checked_validations(
            model.semantic_manifest
        )
