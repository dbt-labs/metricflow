test_name: test_subtract_time_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the SqlSubtractTimeIntervalExpression in a query.
sql_engine: DuckDB
---
-- Test Subtract Time Expression
SELECT
  '2020-01-01' - INTERVAL 3 month AS subtract_time
FROM foo.bar a
