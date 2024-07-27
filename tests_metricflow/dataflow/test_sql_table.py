from __future__ import annotations

import pytest
from metricflow_semantics.sql.sql_table import SqlTable


def test_sql_table() -> None:  # noqa: D103
    sql_table = SqlTable(schema_name="foo", table_name="bar")

    assert sql_table.sql == "foo.bar"
    assert SqlTable.from_string("foo.bar") == sql_table


def test_sql_table_with_db() -> None:  # noqa: D103
    sql_table = SqlTable(db_name="db", schema_name="foo", table_name="bar")

    assert sql_table.sql == "db.foo.bar"
    assert SqlTable.from_string("db.foo.bar") == sql_table


def test_invalid_sql_table() -> None:  # noqa: D103
    with pytest.raises(RuntimeError):
        SqlTable.from_string("foo")
