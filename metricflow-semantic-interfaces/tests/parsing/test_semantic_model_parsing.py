from __future__ import annotations

import textwrap

from metricflow_semantic_interfaces.parsing.dir_to_model import (
    parse_yaml_files_to_semantic_manifest,
)
from metricflow_semantic_interfaces.parsing.objects import YamlConfigFile
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    TimeGranularity,
)

from tests.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)


def test_base_semantic_model_parsing() -> None:
    """Test parsing base attributes of PydanticSemanticModel object."""
    description = "Test semantic_model description"
    label = "Base Test"
    yaml_contents = textwrap.dedent(
        f"""\
        semantic_model:
          name: base_test
          description: {description}
          label: {label}
          node_relation:
            alias: source_table
            schema_name: some_schema
          config:
            meta:
              test_metadata: random
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert semantic_model.description == description
    assert semantic_model.label == label


def test_semantic_model_metadata_parsing() -> None:
    """Test for asserting that internal metadata is parsed into the SemanticModel object."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: metadata_test
          node_relation:
            alias: source_table
            schema_name: some_schema
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert semantic_model.metadata is not None
    assert semantic_model.metadata.repo_file_path == "test_dir/inline_for_test"
    assert semantic_model.metadata.file_slice.filename == "inline_for_test"
    expected_metadata_content = textwrap.dedent(
        """\
        name: metadata_test
        node_relation:
          alias: source_table
          schema_name: some_schema
        """
    )
    assert semantic_model.metadata.file_slice.content == expected_metadata_content


def test_semantic_model_node_relation_parsing() -> None:
    """Test for parsing a semantic model specification with a node_relation provided."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: sql_table_test
          node_relation:
            alias: source_table
            schema_name: some_schema
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert semantic_model.node_relation.relation_name == "some_schema.source_table"


def test_base_semantic_model_entity_parsing() -> None:
    """Test parsing base attributes of PydanticEntity object."""
    label = "Base Test Entity"
    yaml_contents = textwrap.dedent(
        f"""\
        semantic_model:
          name: base_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          entities:
            - name: test_base_entity
              type: primary
              role: test_role
              expr: example_id
              label: {label}
              config:
                meta:
                  random: metadata
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])
    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.entities) == 1
    entity = semantic_model.entities[0]
    assert entity.label == label


def test_semantic_model_entity_parsing() -> None:
    """Test for parsing a basic entity out of a semantic model specification."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: entity_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          entities:
            - name: example_entity
              type: primary
              role: test_role
              expr: example_id
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.entities) == 1
    entity = semantic_model.entities[0]
    assert entity.name == "example_entity"
    assert entity.type is EntityType.PRIMARY
    assert entity.role == "test_role"
    assert entity.expr == "example_id"


def test_semantic_model_entity_metadata_parsing() -> None:
    """Test for parsing metadata for an entity object defined in a semantic model specification."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: entity_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          entities:
            - name: example_entity
              type: primary
              role: test_role
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.entities) == 1
    entity = semantic_model.entities[0]
    assert entity.metadata is not None
    assert entity.metadata.repo_file_path == "test_dir/inline_for_test"
    assert entity.metadata.file_slice.filename == "inline_for_test"
    expected_metadata_content = textwrap.dedent(
        """\
      name: example_entity
      type: primary
      role: test_role
      """
    )
    assert entity.metadata.file_slice.content == expected_metadata_content


def test_base_semantic_model_measure_parsing() -> None:
    """Test parsing base attributes of PydanticMeasure object."""
    description = "Test semantic_model measure description"
    label = "Base Test Measure"
    yaml_contents = textwrap.dedent(
        f"""\
        semantic_model:
          name: base_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          measures:
            - name: example_measure
              agg: count_distinct
              expr: example_input
              description: {description}
              label: {label}
              config:
                meta:
                  random: metadata
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.measures) == 1
    measure = semantic_model.measures[0]
    assert measure.description == description
    assert measure.label == label


def test_semantic_model_measure_parsing() -> None:
    """Test for parsing a measure out of a semantic model specification."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: measure_parsing_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          measures:
            - name: example_measure
              agg: count_distinct
              expr: example_input
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.measures) == 1
    measure = semantic_model.measures[0]
    assert measure.name == "example_measure"
    assert measure.agg is AggregationType.COUNT_DISTINCT
    assert measure.create_metric is not True
    assert measure.expr == "example_input"


def test_semantic_model_measure_metadata_parsing() -> None:
    """Test for parsing metadata for a measure object defined in a semantic model specification."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: measure_metadata_parsing_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          measures:
            - name: example_measure_with_metadata
              agg: count_distinct
              expr: example_input
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.measures) == 1
    measure = semantic_model.measures[0]
    assert measure.metadata is not None
    assert measure.metadata.repo_file_path == "test_dir/inline_for_test"
    assert measure.metadata.file_slice.filename == "inline_for_test"
    expected_metadata_content = textwrap.dedent(
        """\
      name: example_measure_with_metadata
      agg: count_distinct
      expr: example_input
      """
    )
    assert measure.metadata.file_slice.content == expected_metadata_content


def test_semantic_model_create_metric_measure_parsing() -> None:
    """Test for parsing a measure out of a semantic model specification when create metric is set."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: measure_parsing_create_metric_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          measures:
            - name: example_measure
              agg: count_distinct
              create_metric: true
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.measures) == 1
    measure = semantic_model.measures[0]
    assert measure.create_metric is True


def test_semantic_model_categorical_dimension_parsing() -> None:
    """Test for parsing a categorical dimension out of a semantic model specification."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: dimension_parsing_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          dimensions:
            - name: example_categorical_dimension
              type: categorical
              expr: dimension_input
              config:
                meta:
                  random: metadata
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.dimensions) == 1
    dimension = semantic_model.dimensions[0]
    assert dimension.name == "example_categorical_dimension"
    assert dimension.type is DimensionType.CATEGORICAL
    assert dimension.is_partition is not True


def test_semantic_model_partition_dimension_parsing() -> None:
    """Test for parsing a partition dimension out of a semantic model specification."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: dimension_parsing_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          dimensions:
            - name: example_categorical_dimension
              type: categorical
              is_partition: true
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.dimensions) == 1
    dimension = semantic_model.dimensions[0]
    assert dimension.is_partition is True


def test_semantic_model_time_dimension_parsing() -> None:
    """Test for parsing a time dimension out of a semantic model specification."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: dimension_parsing_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          dimensions:
            - name: example_time_dimension
              type: time
              type_params:
                time_granularity: month
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.dimensions) == 1
    dimension = semantic_model.dimensions[0]
    assert dimension.type is DimensionType.TIME
    assert dimension.type_params is not None
    assert dimension.type_params.time_granularity is TimeGranularity.MONTH


def test_semantic_model_primary_time_dimension_parsing() -> None:
    """Test for parsing a primary time dimension out of a semantic model specification."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: dimension_parsing_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          dimensions:
            - name: example_time_dimension
              type: time
              type_params:
                time_granularity: month
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.dimensions) == 1
    dimension = semantic_model.dimensions[0]
    assert dimension.type is DimensionType.TIME
    assert dimension.type_params is not None


def test_base_semantic_model_dimension_parsing() -> None:
    """Test parsing base attributes of PydanticDimension object."""
    description = "Test semantic_model dimension description"
    label = "Base Test Dimension"
    yaml_contents = textwrap.dedent(
        f"""\
        semantic_model:
          name: base_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          dimensions:
            - name: base_dimension_test
              type: categorical
              description: {description}
              label: {label}

        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.dimensions) == 1
    dimension = semantic_model.dimensions[0]
    assert dimension.description == description
    assert dimension.label == label


def test_semantic_model_dimension_metadata_parsing() -> None:
    """Test for parsing metadata for an dimension object defined in a semantic model specification."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: dimension_parsing_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          dimensions:
            - name: example_categorical_dimension
              type: categorical
              expr: dimension_input
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.dimensions) == 1
    dimension = semantic_model.dimensions[0]
    assert dimension.metadata is not None
    assert dimension.metadata.repo_file_path == "test_dir/inline_for_test"
    assert dimension.metadata.file_slice.filename == "inline_for_test"
    expected_metadata_content = textwrap.dedent(
        """\
      name: example_categorical_dimension
      type: categorical
      expr: dimension_input
      """
    )
    assert dimension.metadata.file_slice.content == expected_metadata_content


def test_semantic_model_dimension_validity_params_parsing() -> None:
    """Test for parsing dimension validity info out of a semantic model specification."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: scd_parsing_test
          node_relation:
            alias: source_table
            schema_name: some_schema
          dimensions:
            - name: start_time_dimension
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: True
            - name: end_time_dimension
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_end: True
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert len(semantic_model.dimensions) == 2
    start_dimension = semantic_model.dimensions[0]
    assert start_dimension.type_params is not None
    assert start_dimension.type_params.validity_params is not None
    assert start_dimension.type_params.validity_params.is_start is True
    assert start_dimension.type_params.validity_params.is_end is False
    end_dimension = semantic_model.dimensions[1]
    assert end_dimension.type_params is not None
    assert end_dimension.type_params.validity_params is not None
    assert end_dimension.type_params.validity_params.is_start is False
    assert end_dimension.type_params.validity_params.is_end is True


def test_semantic_model_element_config_merging() -> None:
    """Test for merging element config metadata from semantic model into dimension, entity, and measure objects."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: sm
          config:
            meta:
              sm_metadata: asdf
          node_relation:
            alias: source_table
            schema_name: some_schema
          dimensions:
            - name: dim_0
              type: time
              type_params:
                time_granularity: day
              config:
                meta:
                  sm_metadata: qwer
                  dim_metadata: fdsa
            - name: dim_1
              type: time
              type_params:
                time_granularity: day
              config:
                meta:
                  dim_metadata: mlkj
                  sm_metadata: zxcv
            - name: dim_2
              type: time
              type_params:
                time_granularity: day
            - name: dim_3
              type: time
              type_params:
                time_granularity: day
              config:
                meta:
                  dim_metadata: gfds
          entities:
            - name: entity_0
              type: primary
              config:
                meta:
                  sm_metadata: hjkl
          measures:
            - name: measure_0
              agg: count_distinct
              config:
                meta:
                  sm_metadata: ijkl
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_semantic_manifest(files=[file, EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE])

    assert len(build_result.semantic_manifest.semantic_models) == 1
    semantic_model = build_result.semantic_manifest.semantic_models[0]
    assert semantic_model.config is not None
    assert semantic_model.config.meta["sm_metadata"] == "asdf"
    assert len(semantic_model.dimensions) == 4
    assert semantic_model.dimensions[0].config is not None
    assert semantic_model.dimensions[0].config.meta["sm_metadata"] == "qwer"
    assert semantic_model.dimensions[0].config.meta["dim_metadata"] == "fdsa"
    assert semantic_model.dimensions[1].config is not None
    assert semantic_model.dimensions[1].config.meta["sm_metadata"] == "zxcv"
    assert semantic_model.dimensions[1].config.meta["dim_metadata"] == "mlkj"
    assert semantic_model.dimensions[2].config is not None
    assert semantic_model.dimensions[2].config.meta["sm_metadata"] == "asdf"
    assert semantic_model.dimensions[3].config is not None
    assert semantic_model.dimensions[3].config.meta["dim_metadata"] == "gfds"
    assert semantic_model.dimensions[3].config.meta["sm_metadata"] == "asdf"
    assert len(semantic_model.entities) == 1
    assert semantic_model.entities[0].config is not None
    assert semantic_model.entities[0].config.meta["sm_metadata"] == "hjkl"
    assert len(semantic_model.measures) == 1
    assert semantic_model.measures[0].config is not None
    assert semantic_model.measures[0].config.meta["sm_metadata"] == "ijkl"
