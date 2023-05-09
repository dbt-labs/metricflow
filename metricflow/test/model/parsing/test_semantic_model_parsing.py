import textwrap

from dbt_semantic_interfaces.objects.aggregation_type import AggregationType
from dbt_semantic_interfaces.objects.elements.entity import EntityType
from dbt_semantic_interfaces.objects.elements.dimension import DimensionType
from dbt_semantic_interfaces.parsing.dir_to_model import parse_yaml_files_to_model
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.objects.time_granularity import TimeGranularity


def test_semantic_model_metadata_parsing() -> None:
    """Test for asserting that internal metadata is parsed into the SemanticModel object"""
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

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.semantic_models) == 1
    semantic_model = build_result.model.semantic_models[0]
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
    """Test for parsing a semantic model specification with a node_relation provided"""
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

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.semantic_models) == 1
    semantic_model = build_result.model.semantic_models[0]
    assert semantic_model.node_relation.relation_name == "some_schema.source_table"


def test_semantic_model_entity_parsing() -> None:
    """Test for parsing a basic entity out of a semantic model specification"""
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

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.semantic_models) == 1
    semantic_model = build_result.model.semantic_models[0]
    assert len(semantic_model.entities) == 1
    entity = semantic_model.entities[0]
    assert entity.name == "example_entity"
    assert entity.type is EntityType.PRIMARY
    assert entity.role == "test_role"
    assert entity.expr == "example_id"


def test_semantic_model_entity_metadata_parsing() -> None:
    """Test for parsing metadata for an entity object defined in a semantic model specification"""
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

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.semantic_models) == 1
    semantic_model = build_result.model.semantic_models[0]
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


def test_semantic_model_measure_parsing() -> None:
    """Test for parsing a measure out of a semantic model specification"""
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

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.semantic_models) == 1
    semantic_model = build_result.model.semantic_models[0]
    assert len(semantic_model.measures) == 1
    measure = semantic_model.measures[0]
    assert measure.name == "example_measure"
    assert measure.agg is AggregationType.COUNT_DISTINCT
    assert measure.create_metric is not True
    assert measure.expr == "example_input"


def test_semantic_model_measure_metadata_parsing() -> None:
    """Test for parsing metadata for a measure object defined in a semantic model specification"""
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

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.semantic_models) == 1
    semantic_model = build_result.model.semantic_models[0]
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
    """Test for parsing a measure out of a semantic model specification when create metric is set"""
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

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.semantic_models) == 1
    semantic_model = build_result.model.semantic_models[0]
    assert len(semantic_model.measures) == 1
    measure = semantic_model.measures[0]
    assert measure.create_metric is True


def test_semantic_model_categorical_dimension_parsing() -> None:
    """Test for parsing a categorical dimension out of a semantic model specification"""
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
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.semantic_models) == 1
    semantic_model = build_result.model.semantic_models[0]
    assert len(semantic_model.dimensions) == 1
    dimension = semantic_model.dimensions[0]
    assert dimension.name == "example_categorical_dimension"
    assert dimension.type is DimensionType.CATEGORICAL
    assert dimension.is_partition is not True


def test_semantic_model_partition_dimension_parsing() -> None:
    """Test for parsing a partition dimension out of a semantic model specification"""
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

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.semantic_models) == 1
    semantic_model = build_result.model.semantic_models[0]
    assert len(semantic_model.dimensions) == 1
    dimension = semantic_model.dimensions[0]
    assert dimension.is_partition is True


def test_semantic_model_time_dimension_parsing() -> None:
    """Test for parsing a time dimension out of a semantic model specification"""
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

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.semantic_models) == 1
    semantic_model = build_result.model.semantic_models[0]
    assert len(semantic_model.dimensions) == 1
    dimension = semantic_model.dimensions[0]
    assert dimension.type is DimensionType.TIME
    assert dimension.type_params is not None
    assert dimension.type_params.is_primary is not True
    assert dimension.type_params.time_granularity is TimeGranularity.MONTH


def test_semantic_model_primary_time_dimension_parsing() -> None:
    """Test for parsing a primary time dimension out of a semantic model specification"""
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
                is_primary: true
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.semantic_models) == 1
    semantic_model = build_result.model.semantic_models[0]
    assert len(semantic_model.dimensions) == 1
    dimension = semantic_model.dimensions[0]
    assert dimension.type is DimensionType.TIME
    assert dimension.type_params is not None
    assert dimension.type_params.is_primary is True


def test_semantic_model_dimension_metadata_parsing() -> None:
    """Test for parsing metadata for an dimension object defined in a semantic model specification"""
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

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.semantic_models) == 1
    semantic_model = build_result.model.semantic_models[0]
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
    """Test for parsing dimension validity info out of a semantic model specification"""
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

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.semantic_models) == 1
    semantic_model = build_result.model.semantic_models[0]
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
