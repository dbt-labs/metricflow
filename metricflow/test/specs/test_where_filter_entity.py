import pytest
from metricflow.errors.errors import InvalidQuerySyntax
from metricflow.specs.where_filter_entity import WhereFilterEntity


def test_descending_cannot_be_set():
    with pytest.raises(InvalidQuerySyntax):
        WhereFilterEntity("customer").descending(True)
