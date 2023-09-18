from abc import abstractmethod
from typing_extensions import override
import pytest
from metricflow.errors.errors import InvalidQuerySyntax
from metricflow.specs.where_filter_time_dimension import WhereFilterTimeDimensionFactory
from dbt_semantic_interfaces.call_parameter_sets import FilterCallParameterSets
from metricflow.specs.column_assoc import ColumnAssociationResolver
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow.specs.specs import InstanceSpec
from metricflow.specs.column_assoc import ColumnAssociation


class MockColumnAssociationResolver(ColumnAssociationResolver):
    """Just a mock for testing."""

    @abstractmethod
    def resolve_spec(self, spec: InstanceSpec) -> ColumnAssociation:  # noqa: D
        raise NotImplementedError


def test_descending_cannot_be_set():  # noqa
    with pytest.raises(InvalidQuerySyntax):
        WhereFilterTimeDimensionFactory(FilterCallParameterSets(), MockColumnAssociationResolver).create(
            "metric_time", TimeGranularity.WEEK, descending=True
        )
