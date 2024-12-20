test_name: test_add_time_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the SqlAddTimeExpr in a query.
sql_engine: Redshift
---
-- Test Add Time Expression
SELECT
  DATEADD(month, (1 * 3), '2020-01-01') AS add_time
FROM foo.bar a
