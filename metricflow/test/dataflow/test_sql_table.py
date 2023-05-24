from __future__ import annotations

import pytest

from metricflow.dataflow.sql_table import SqlTable


def test_sql_table() -> None:  # noqa: D
    sql_table = SqlTable(schema_name="foo", table_name="bar")

    assert sql_table.sql == "foo.bar"
    assert SqlTable.from_string("foo.bar") == sql_table

    json_serialized_table = sql_table.json()
    deserialized_table = SqlTable.parse_raw(json_serialized_table)

    assert sql_table == deserialized_table


def test_sql_table_with_db() -> None:  # noqa: D
    sql_table = SqlTable(db_name="db", schema_name="foo", table_name="bar")

    assert sql_table.sql == "db.foo.bar"
    assert SqlTable.from_string("db.foo.bar") == sql_table

    json_serialized_table = sql_table.json()
    deserialized_table = SqlTable.parse_raw(json_serialized_table)

    assert sql_table == deserialized_table


def test_invalid_sql_table() -> None:  # noqa: D
    with pytest.raises(RuntimeError):
        SqlTable.from_string("foo")
