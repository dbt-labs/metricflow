from metricflow.model.objects.constraints.where import WhereClauseConstraint


def test_where_constraint_parsing() -> None:
    """Simple test of a parsed where with a function and some boolean and equality checks"""
    parsed_where = WhereClauseConstraint.parse("ds >= CAST('2020-01-01' AS TIMESTAMP) AND is_instant")
    assert set(parsed_where.linkable_names) == {"ds", "is_instant"}


def test_where_constraint_parsing_empty_function() -> None:
    """Test involving an empty function, which produces an empty object"""
    parsed_where = WhereClauseConstraint.parse("is_internal IS FALSE AND is_instant = false AND ds = CURRENT_DATE()")
    assert set(parsed_where.linkable_names) == {"is_internal", "is_instant", "ds"}


def test_where_constraint_with_between() -> None:
    """Testing a where constraint with a BETWEEN expression"""
    parsed_where = WhereClauseConstraint.parse("WHERE ds < CURRENT_DATE() AND price BETWEEN min_price AND 1.50")
    assert set(parsed_where.linkable_names) == {"ds", "price", "min_price"}
