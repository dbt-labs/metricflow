import textwrap

import pytest
from metricflow.dataflow.sql_table import SqlTable

from metricflow.errors.errors import ParsingException
from metricflow.model.objects.materialization import (
    MaterializationFormat,
    MaterializationLocation,
    MaterializationTableauParams,
)
from metricflow.model.parsing.dir_to_model import parse_yaml_files_to_model
from metricflow.model.parsing.yaml_file import YamlFile


def test_simple_materialization_parsing() -> None:
    """Test for parsing a simple maeterialization specification with no optional parameters"""
    yaml_contents = textwrap.dedent(
        """\
        materialization:
          name: simple_materialization_test
          metrics:
            - some_metric
          dimensions:
            - some_dimension
            - time_dimension
        """
    )
    file = YamlFile(file_path="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.materializations) == 1
    materialization = model.materializations[0]
    assert materialization.name == "simple_materialization_test"
    assert materialization.metrics == ["some_metric"]
    assert materialization.dimensions == ["some_dimension", "time_dimension"]


def test_materialization_metadata_parsing() -> None:
    """Test for asserting that internal metadata is parsed into the Materialization object"""
    yaml_contents = textwrap.dedent(
        """\
        materialization:
          name: simple_materialization_test
          metrics:
            - some_metric
          dimensions:
            - some_dimension
            - time_dimension
        """
    )
    file = YamlFile(file_path="test_dir/inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.materializations) == 1
    materialization = model.materializations[0]
    assert materialization.metadata is not None
    assert materialization.metadata.repo_file_path == "test_dir/inline_for_test"
    assert materialization.metadata.file_slice.filename == "inline_for_test"


def test_materialization_with_simple_destinations_parsing() -> None:
    """Test for parsing a materialization specification with destinations set"""
    yaml_contents = textwrap.dedent(
        """\
        materialization:
          name: materialization_simple_destinations_test
          metrics:
            - some_metric
          dimensions:
            - some_dimension
          destinations:
            - location: dw
              format: wide
        """
    )
    file = YamlFile(file_path="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.materializations) == 1
    materialization = model.materializations[0]
    assert materialization.destinations is not None
    assert len(materialization.destinations) == 1
    destination = materialization.destinations[0]
    assert destination.location is MaterializationLocation.DW
    assert destination.format is MaterializationFormat.WIDE


def test_materialization_with_rollup_destination_parsing() -> None:
    """Test for parsing a materialization specification with a rollup set on a destination"""
    yaml_contents = textwrap.dedent(
        """\
        materialization:
          name: materialization_simple_destinations_test
          metrics:
            - some_metric
          dimensions:
            - some_dimension
          destinations:
            - location: dw
              format: wide
              rollups:
               - []
               - ["some_dimension", "*"]
               - ["some_dimension"]
        """
    )
    file = YamlFile(file_path="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.materializations) == 1
    materialization = model.materializations[0]
    assert materialization.destinations is not None
    assert len(materialization.destinations) == 1
    destination = materialization.destinations[0]
    assert destination.rollups is not None
    assert destination.rollups == [[], ["some_dimension", "*"], ["some_dimension"]]


def test_materialization_with_indented_rollup_destination_parsing() -> None:
    """Test for parsing a materialization specification with a rollup set on a destination"""
    yaml_contents = textwrap.dedent(
        """\
        materialization:
          name: materialization_simple_destinations_test
          metrics:
            - some_metric
          dimensions:
            - some_dimension
          destinations:
            - location: dw
              format: wide
              rollups:
               -
                 - "some_dimension"
                 - "*"
               -
                 - "some_dimension"
        """
    )
    file = YamlFile(file_path="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.materializations) == 1
    materialization = model.materializations[0]
    assert materialization.destinations is not None
    assert len(materialization.destinations) == 1
    destination = materialization.destinations[0]
    assert destination.rollups is not None
    assert destination.rollups == [["some_dimension", "*"], ["some_dimension"]]


def test_materialization_with_tableau_parameters_parsing() -> None:
    """Test for parsing a materialization specification with Tableau parameters"""
    yaml_contents = textwrap.dedent(
        """\
        materialization:
          name: materialization_simple_destinations_test
          metrics:
            - some_metric
          dimensions:
            - some_dimension
          destinations:
            - location: dw
              format: wide
              tableau_params:
                projects:
                 - some_project
        """
    )
    file = YamlFile(file_path="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.materializations) == 1
    materialization = model.materializations[0]
    assert materialization.destinations is not None
    assert len(materialization.destinations) == 1
    destination = materialization.destinations[0]
    assert destination.tableau_params == MaterializationTableauParams(projects=["some_project"])


def test_materialization_with_destination_table_parsing() -> None:
    """Test for parsing a materialization specification with a destination table set"""
    yaml_contents = textwrap.dedent(
        """\
        materialization:
          name: materialization_simple_destinations_test
          metrics:
            - some_metric
          dimensions:
            - some_dimension
          destinations:
            - location: dw
              format: wide
          destination_table: "bq_project.output_schema.materialization_table"
        """
    )
    file = YamlFile(file_path="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.materializations) == 1
    materialization = model.materializations[0]
    assert materialization.destinations is not None and len(materialization.destinations) == 1
    assert materialization.destination_table is not None
    assert materialization.destination_table == SqlTable(
        db_name="bq_project", schema_name="output_schema", table_name="materialization_table"
    )


def test_materialization_with_invalid_destination_table_parsing_error() -> None:
    """Test for parsing error thrown when the sql table name cannot be properly parsed"""
    yaml_contents = textwrap.dedent(
        """\
        materialization:
          name: materialization_simple_destinations_test
          metrics:
            - some_metric
          dimensions:
            - some_dimension
          destinations:
            - location: dw
              format: wide
          destination_table: "this is an invalid table name"
        """
    )
    file = YamlFile(file_path="inline_for_test", contents=yaml_contents)

    with pytest.raises(ParsingException, match="Invalid input for a SQL table"):
        parse_yaml_files_to_model(files=[file])
