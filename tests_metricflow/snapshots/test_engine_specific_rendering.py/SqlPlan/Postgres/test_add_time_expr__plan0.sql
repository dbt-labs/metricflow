test_name: test_add_time_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the SqlAddTimeExpr in a query.
sql_engine: Postgres
---
-- Test Add Time Expression
SELECT
  '2020-01-01' + MAKE_INTERVAL(months => (1 * 3)) AS add_time
FROM foo.bar a
