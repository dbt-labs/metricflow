import textwrap

from metricflow.aggregation_properties import AggregationType
from metricflow.model.objects.common import YamlConfigFile
from dbt.contracts.graph.entities import EntityOrigin, MutabilityType
from dbt.contracts.graph.identifiers import IdentifierType
from dbt.contracts.graph.dimensions import DimensionType
from metricflow.model.parsing.dir_to_model import parse_yaml_files_to_model
from dbt.semantic.time import TimeGranularity


def test_base_entity_attribute_parsing() -> None:
    """Test for parsing a entity specification without regard for measures, identifiers, or dimensions"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: base_property_test
          mutability:
            type: append_only
            type_params:
              min: minimum_value
              max: maximum_value
              update_cron: "* * 1 * *"
              along: dimension_column
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert entity.name == "base_property_test"
    assert entity.origin == EntityOrigin.SOURCE  # auto-filled from default, not user-configurable
    assert entity.mutability.type == MutabilityType.APPEND_ONLY
    assert entity.mutability.type_params is not None
    assert entity.mutability.type_params.min == "minimum_value"
    assert entity.mutability.type_params.max == "maximum_value"
    assert entity.mutability.type_params.update_cron == "* * 1 * *"
    assert entity.mutability.type_params.along == "dimension_column"


def test_entity_metadata_parsing() -> None:
    """Test for asserting that internal metadata is parsed into the Entity object"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: metadata_test
          mutability:
            type: immutable
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert entity.metadata is not None
    assert entity.metadata.repo_file_path == "test_dir/inline_for_test"
    assert entity.metadata.file_slice.filename == "inline_for_test"
    expected_metadata_content = textwrap.dedent(
        """\
        name: metadata_test
        mutability:
          type: immutable
        """
    )
    assert entity.metadata.file_slice.content == expected_metadata_content


def test_entity_sql_table_parsing() -> None:
    """Test for parsing a entity specification with a sql_table provided"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: sql_table_test
          mutability:
            type: immutable
          sql_table: "some_schema.source_table"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert entity.sql_table == "some_schema.source_table"


def test_entity_sql_query_parsing() -> None:
    """Test for parsing a entity specification with a sql_query provided"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: sql_query_test
          mutability:
            type: immutable
          sql_query: "SELECT * FROM some_schema.source_table"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert entity.sql_query == "SELECT * FROM some_schema.source_table"


def test_entity_identifier_parsing() -> None:
    """Test for parsing a basic identifier out of a entity specification"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: identifier_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              entity: other_identifier
              expr: example_id
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.identifiers) == 1
    identifier = entity.identifiers[0]
    assert identifier.name == "example_identifier"
    assert identifier.type is IdentifierType.PRIMARY
    assert identifier.role == "test_role"
    assert identifier.entity == "other_identifier"
    assert identifier.expr == "example_id"


def test_entity_identifier_metadata_parsing() -> None:
    """Test for parsing metadata for an identifier object defined in a entity specification"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: identifier_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.identifiers) == 1
    identifier = entity.identifiers[0]
    assert identifier.metadata is not None
    assert identifier.metadata.repo_file_path == "test_dir/inline_for_test"
    assert identifier.metadata.file_slice.filename == "inline_for_test"
    expected_metadata_content = textwrap.dedent(
        """\
      name: example_identifier
      type: primary
      role: test_role
      """
    )
    assert identifier.metadata.file_slice.content == expected_metadata_content


def test_entity_identifier_default_entity_parsing() -> None:
    """Test for parsing an identifier with no entity specified out of a entity specification"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: entity_default_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          identifiers:
            - name: example_default_entity_identifier
              type: primary
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.identifiers) == 1
    identifier = entity.identifiers[0]
    assert identifier.entity == "example_default_entity_identifier"


def test_entity_composite_sub_identifier_ref_parsing() -> None:
    """Test for parsing a ref-based composite sub-identifier out of a entity specification"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: composite_sub_identifier_ref_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              identifiers:
                - name: composite_ref_identifier
                  ref: other_identifier
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.identifiers) == 1
    identifier = entity.identifiers[0]
    assert len(identifier.identifiers) == 1

    ref_sub_identifier = identifier.identifiers[0]
    assert ref_sub_identifier.name == "composite_ref_identifier"
    assert ref_sub_identifier.ref == "other_identifier"
    assert ref_sub_identifier.expr is None


def test_entity_composite_sub_identifier_expr_parsing() -> None:
    """Test for parsing an expr-based composite sub-identifier out of a entity specification"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: composite_sub_identifier_expr_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              identifiers:
                - name: composite_expr_identifier
                  expr: CAST(expr_identifier AS BIGINT)
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.identifiers) == 1
    identifier = entity.identifiers[0]
    assert len(identifier.identifiers) == 1

    expr_sub_identifier = identifier.identifiers[0]
    assert expr_sub_identifier.name == "composite_expr_identifier"
    assert expr_sub_identifier.expr == "CAST(expr_identifier AS BIGINT)"
    assert expr_sub_identifier.ref is None


def test_entity_measure_parsing() -> None:
    """Test for parsing a measure out of a entity specification"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: measure_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          measures:
            - name: example_measure
              agg: count_distinct
              expr: example_input
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.measures) == 1
    measure = entity.measures[0]
    assert measure.name == "example_measure"
    assert measure.agg is AggregationType.COUNT_DISTINCT
    assert measure.create_metric is not True
    assert measure.expr == "example_input"


def test_entity_measure_metadata_parsing() -> None:
    """Test for parsing metadata for a measure object defined in a entity specification"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: measure_metadata_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          measures:
            - name: example_measure_with_metadata
              agg: count_distinct
              expr: example_input
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.measures) == 1
    measure = entity.measures[0]
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


def test_entity_create_metric_measure_parsing() -> None:
    """Test for parsing a measure out of a entity specification when create metric is set"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: measure_parsing_create_metric_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          measures:
            - name: example_measure
              agg: count_distinct
              create_metric: true
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.measures) == 1
    measure = entity.measures[0]
    assert measure.create_metric is True


def test_entity_categorical_dimension_parsing() -> None:
    """Test for parsing a categorical dimension out of a entity specification"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: dimension_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          dimensions:
            - name: example_categorical_dimension
              type: categorical
              expr: dimension_input
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.dimensions) == 1
    dimension = entity.dimensions[0]
    assert dimension.name == "example_categorical_dimension"
    assert dimension.type is DimensionType.CATEGORICAL
    assert dimension.is_partition is not True


def test_entity_partition_dimension_parsing() -> None:
    """Test for parsing a partition dimension out of a entity specification"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: dimension_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          dimensions:
            - name: example_categorical_dimension
              type: categorical
              is_partition: true
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.dimensions) == 1
    dimension = entity.dimensions[0]
    assert dimension.is_partition is True


def test_entity_time_dimension_parsing() -> None:
    """Test for parsing a time dimension out of a entity specification"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: dimension_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          dimensions:
            - name: example_time_dimension
              type: time
              type_params:
                time_format: "%Y-%M-%D"
                time_granularity: month
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.dimensions) == 1
    dimension = entity.dimensions[0]
    assert dimension.type is DimensionType.TIME
    assert dimension.type_params is not None
    assert dimension.type_params.is_primary is not True
    assert dimension.type_params.time_format == "%Y-%M-%D"
    assert dimension.type_params.time_granularity is TimeGranularity.MONTH


def test_entity_primary_time_dimension_parsing() -> None:
    """Test for parsing a primary time dimension out of a entity specification"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: dimension_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
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

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.dimensions) == 1
    dimension = entity.dimensions[0]
    assert dimension.type is DimensionType.TIME
    assert dimension.type_params is not None
    assert dimension.type_params.is_primary is True


def test_entity_dimension_metadata_parsing() -> None:
    """Test for parsing metadata for an dimension object defined in a entity specification"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: dimension_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          dimensions:
            - name: example_categorical_dimension
              type: categorical
              expr: dimension_input
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.dimensions) == 1
    dimension = entity.dimensions[0]
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


def test_entity_dimension_validity_params_parsing() -> None:
    """Test for parsing dimension validity info out of a entity specification"""
    yaml_contents = textwrap.dedent(
        """\
        entity:
          name: scd_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
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

    assert len(build_result.model.entities) == 1
    entity = build_result.model.entities[0]
    assert len(entity.dimensions) == 2
    start_dimension = entity.dimensions[0]
    assert start_dimension.type_params is not None
    assert start_dimension.type_params.validity_params is not None
    assert start_dimension.type_params.validity_params.is_start is True
    assert start_dimension.type_params.validity_params.is_end is False
    end_dimension = entity.dimensions[1]
    assert end_dimension.type_params is not None
    assert end_dimension.type_params.validity_params is not None
    assert end_dimension.type_params.validity_params.is_start is False
    assert end_dimension.type_params.validity_params.is_end is True
