test_name: test_cast_to_timestamp
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the cast to timestamp expression in a query.
sql_engine: Clickhouse
---
-- Test Cast to Timestamp Expression
SELECT
  toDateTime64('2020-01-01') AS col0
FROM foo.bar a
