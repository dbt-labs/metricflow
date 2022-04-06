import pytest

from metricflow.model.objects.constraints.where import WhereClauseConstraint


@pytest.mark.skip(reason="Does not currently parse correctly.")
def test_where_constraint_parsing() -> None:  # noqa: D
    """Currently throws an exception:

    ConstraintParseException: expected parsed constraint to contain exactly one key; got {}
    """
    parsed_where = WhereClauseConstraint.parse("ds = CAST('2020-01-01' AS TIMESTAMP) AND is_instant")
    assert set(parsed_where.linkable_names) == {"ds", "is_instant"}
