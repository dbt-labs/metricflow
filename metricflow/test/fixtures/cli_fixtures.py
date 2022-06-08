from __future__ import annotations

import click
import datetime
import pandas as pd
import pytest

from click.testing import CliRunner, Result
from typing import List, Optional, Sequence

from metricflow.cli.cli_context import CLIContext
from metricflow.engine.models import Dimension, Materialization, Metric
from metricflow.dataflow.sql_table import SqlTable
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest, MetricFlowQueryResult
from metricflow.model.objects.data_source import DataSource, Mutability, MutabilityType
from metricflow.model.objects.elements.dimension import Dimension as DimensionObj, DimensionType, DimensionTypeParams
from metricflow.model.objects.metric import Metric as MetricObj
from metricflow.specs import DimensionReference
from metricflow.time.time_granularity import TimeGranularity


class MockMetricFlowEngine:
    """Mock MetricFlowEngine class as only integration is needed to be tested."""

    def query(self, mf_request: MetricFlowQueryRequest) -> MetricFlowQueryResult:  # noqa: D
        data = {"metric1": [123, 5123, 23, 5123], "ds": [1, 2, 3, 4]}
        return MetricFlowQueryResult(
            query_spec=None,  # type: ignore
            dataflow_plan=None,  # type: ignore
            sql="SELECT * FROM hello",
            result_df=pd.DataFrame(data),
            result_table=None,
        )

    def simple_dimensions_for_metrics(self, metric_names: List[str]) -> List[Dimension]:  # noqa: D
        dimensions = ["dim1", "dim2", "dim3"]
        return [Dimension(name) for name in dimensions]

    def list_metrics(self) -> List[Metric]:  # noqa: D
        return [Metric(name="metric1", dimensions=self.simple_dimensions_for_metrics(metric_names=[]))]

    def get_dimension_values(  # noqa: D
        self,
        metric_name: str,
        get_group_by_values: str,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
    ) -> List[str]:
        return ["dim_val1", "dim_val2", "dim_val3"]

    def list_materializations(self) -> List[Materialization]:  # noqa: D
        return [Materialization(name="mat1", metrics=["metric1"], dimensions=["dim1", "dim2"], destination_table=None)]

    def materialize(  # noqa: D
        self,
        materialization_name: str,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
    ) -> SqlTable:
        return SqlTable.from_string("test.table")

    def drop_materialization(self, materialization_name: str) -> bool:  # noqa: D
        return True


class MockSemanticModel:
    """Mock SemanticModel class as only integration is needed to be tested."""

    @property
    def user_configured_model(self) -> str:  # type: ignore
        """Mocked UserConfiguredModel."""
        return "Should build a mocked model if needed in the future"


class MockUserConfiguredModel:
    """Mock UserConfiguredModel class as only integration is needed to be tested."""

    @property
    def data_sources(self) -> List[DataSource]:  # noqa: D
        return [
            DataSource(
                name="animals",
                sql_query="SELECT true as foo, '2022-06-01' as ds",
                measures=[],
                dimensions=[
                    DimensionObj(
                        name=DimensionReference(element_name="ds"),
                        type=DimensionType.TIME,
                        type_params=DimensionTypeParams(
                            is_primary=True,
                            time_granularity=TimeGranularity.DAY,
                        ),
                    ),
                    DimensionObj(name=DimensionReference(element_name="foo"), type=DimensionType.CATEGORICAL),
                ],
                mutability=Mutability(type=MutabilityType.IMMUTABLE),
            )
        ]

    @property
    def metrics(self) -> List[MetricObj]:  # noqa: D
        return []


class MetricFlowCliRunner(CliRunner):
    """Custom CliRunner class to handle mocks."""

    def run(self, cli: click.BaseCommand, args: Sequence[str] = None) -> Result:  # noqa: D
        # Mock the metricflow engine
        cli_context = CLIContext()
        cli_context._mf = MockMetricFlowEngine()  # type: ignore
        cli_context._semantic_model = MockSemanticModel()  # type: ignore
        cli_context._user_configured_model = MockUserConfiguredModel()  # type: ignore
        return super().invoke(cli, args, obj=cli_context)


@pytest.fixture
def cli_runner() -> MetricFlowCliRunner:  # noqa: D
    return MetricFlowCliRunner()
