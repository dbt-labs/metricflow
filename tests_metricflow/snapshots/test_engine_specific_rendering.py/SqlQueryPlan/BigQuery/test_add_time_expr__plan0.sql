test_name: test_add_time_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the SqlAddTimeExpr in a query.
sql_engine: BigQuery
---
-- Test Add Time Expression
SELECT
  DATE_ADD(CAST('2020-01-01' AS DATETIME), INTERVAL SqlExpressionRenderResult(sql='1', bind_parameter_set=SqlBindParameterSet(param_items=())) quarter) AS add_time
FROM foo.bar a
