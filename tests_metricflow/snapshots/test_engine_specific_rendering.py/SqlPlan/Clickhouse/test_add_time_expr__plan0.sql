test_name: test_add_time_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the SqlAddTimeExpr in a query.
sql_engine: Clickhouse
---
-- Test Add Time Expression
SELECT
  addMonths('2020-01-01', CAST((1) AS Integer)) AS add_time
FROM foo.bar a
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
