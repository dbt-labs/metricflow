test_name: test_add_time_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the SqlAddTimeExpr in a query.
sql_engine: DuckDB
---
-- Test Add Time Expression
SELECT
  '2020-01-01' + INTERVAL (1 * 3) month AS add_time
FROM foo.bar a
