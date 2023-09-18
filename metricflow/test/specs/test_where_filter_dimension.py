from __future__ import annotations

import pytest

from metricflow.errors.errors import InvalidQuerySyntax
from metricflow.specs.where_filter_dimension import WhereFilterDimension


def test_descending_cannot_be_set() -> None:  # noqa
    with pytest.raises(InvalidQuerySyntax):
        WhereFilterDimension("bookings").descending(True)
