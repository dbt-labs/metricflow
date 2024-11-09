test_name: test_approximate_discrete_percentile_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the approximate discrete percentile expression in a query.
sql_engine: Databricks
---
-- Test Approximate Discrete Percentile Expression
SELECT
  APPROX_PERCENTILE(a.col0, 0.5) AS col0_percentile
FROM foo.bar a
