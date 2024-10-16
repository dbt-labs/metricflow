from __future__ import annotations

import pytest
from metricflow_semantics.sql.sql_table import SqlTable


def test_sql_table() -> None:  # noqa: D103
    assert SqlTable(schema_name=None, table_name="table", db_name=None).sql == "table"
    assert SqlTable(schema_name="schema", table_name="table", db_name=None).sql == "schema.table"
    assert SqlTable(schema_name="schema", table_name="table", db_name="db").sql == "db.schema.table"

    with pytest.raises(ValueError):
        SqlTable(schema_name=None, table_name="table", db_name="db")
