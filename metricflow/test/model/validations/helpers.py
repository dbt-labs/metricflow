import textwrap
from typing import List, Optional, Sequence

from metricflow.dataflow.sql_table import SqlTable
from metricflow.engine.models import Dimension
from metricflow.model.objects.common import FileSlice, Metadata, YamlConfigFile
from metricflow.model.objects.constraints.where import WhereClauseConstraint
from metricflow.model.objects.data_source import DataSource, DataSourceOrigin, Mutability
from metricflow.model.objects.elements.identifier import Identifier
from metricflow.model.objects.elements.measure import Measure
from metricflow.model.objects.materialization import Materialization, MaterializationDestination
from metricflow.model.objects.metric import Metric, MetricType, MetricTypeParams


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
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              entity: other_identifier
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


def materialization_with_guaranteed_meta(
    name: str,
    metrics: List[str],
    dimensions: List[str],
    description: str = "adhoc materialization",
    metadata: Metadata = default_meta(),
    destinations: List[MaterializationDestination] = [],
    destination_table: Optional[SqlTable] = None,
) -> Materialization:
    """Creates a materialization with the given input. If a metadata object is not supplied, a default metadata object is used"""

    return Materialization(
        name=name,
        description=description,
        metrics=metrics,
        dimensions=dimensions,
        destinations=destinations,
        destination_table=destination_table,
        metadata=metadata,
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
    mutability: Mutability,
    description: Optional[str] = None,
    sql_table: Optional[str] = None,
    sql_query: Optional[str] = None,
    dbt_model: Optional[str] = None,
    metadata: Metadata = default_meta(),
    identifiers: Sequence[Identifier] = [],
    measures: Sequence[Measure] = [],
    dimensions: Sequence[Dimension] = [],
    origin: DataSourceOrigin = DataSourceOrigin.SOURCE,
) -> DataSource:
    """Creates a data source with the given input. If a metadata object is not supplied, a default metadata object is used"""

    return DataSource(
        name=name,
        mutability=mutability,
        description=description,
        sql_table=sql_table,
        sql_query=sql_query,
        dbt_model=dbt_model,
        identifiers=identifiers,
        measures=measures,
        dimensions=dimensions,
        origin=origin,
        metadata=metadata,
    )
