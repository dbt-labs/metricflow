import textwrap
from typing import Optional, Sequence

from metricflow.engine.models import Dimension
from dbt_semantic_interfaces.objects.metadata import FileSlice, Metadata
from dbt_semantic_interfaces.objects.constraints.where import WhereClauseConstraint
from dbt_semantic_interfaces.objects.data_source import DataSource
from dbt_semantic_interfaces.objects.elements.entity import Entity
from dbt_semantic_interfaces.objects.elements.measure import Measure
from dbt_semantic_interfaces.objects.metric import Metric, MetricType, MetricTypeParams
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile


def base_model_file() -> YamlConfigFile:
    """Returns a YamlConfigFile with the inputs for a basic valid model

    This is useful to seed a simple error-free model, which can easily be extended with YAML inputs
    containing specific validation triggers.
    """
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: sample_data_source
          sql_table: some_schema.source_table
          entities:
            - name: example_entity
              type: primary
              role: test_role
              expr: example_id
          measures:
            - name: num_sample_rows
              agg: sum
              expr: 1
              create_metric: true
          dimensions:
            - name: ds
              type: time
              type_params:
                time_granularity: day
                is_primary: true
        """
    )
    return YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)


def default_meta() -> Metadata:
    """Returns a Metadata object with the required information"""

    return Metadata(
        repo_file_path="/not/from/a/repo",
        file_slice=FileSlice(
            filename="not_from_file.py",
            content="N/A",
            start_line_number=0,
            end_line_number=0,
        ),
    )


def metric_with_guaranteed_meta(
    name: str,
    type: MetricType,
    type_params: MetricTypeParams,
    constraint: Optional[WhereClauseConstraint] = None,
    metadata: Metadata = default_meta(),
    description: str = "adhoc metric",
) -> Metric:
    """Creates a metric with the given input. If a metadata object is not supplied, a default metadata object is used"""

    return Metric(
        name=name,
        description=description,
        type=type,
        type_params=type_params,
        constraint=constraint,
        metadata=metadata,
    )


def data_source_with_guaranteed_meta(
    name: str,
    description: Optional[str] = None,
    sql_table: Optional[str] = None,
    sql_query: Optional[str] = None,
    metadata: Metadata = default_meta(),
    entities: Sequence[Entity] = [],
    measures: Sequence[Measure] = [],
    dimensions: Sequence[Dimension] = [],
) -> DataSource:
    """Creates a data source with the given input. If a metadata object is not supplied, a default metadata object is used"""

    return DataSource(
        name=name,
        description=description,
        sql_table=sql_table,
        sql_query=sql_query,
        entities=entities,
        measures=measures,
        dimensions=dimensions,
        metadata=metadata,
    )
