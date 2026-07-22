test_name: test_approximate_continuous_percentile_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the approximate continuous percentile expression in a query.
sql_engine: Vertica
---
-- Test Approximate Continuous Percentile Expression
SELECT
  APPROXIMATE_PERCENTILE(CAST(a.col0 AS DOUBLE PRECISION) USING PARAMETERS percentile = 0.5) AS col0_percentile
FROM foo.bar a
