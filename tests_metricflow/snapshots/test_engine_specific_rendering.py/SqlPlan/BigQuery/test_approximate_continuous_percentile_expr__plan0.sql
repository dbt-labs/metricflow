test_name: test_approximate_continuous_percentile_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the approximate continuous percentile expression in a query.
sql_engine: BigQuery
---
-- Test Approximate Continuous Percentile Expression
SELECT
  APPROX_QUANTILES(a.col0, 2)[OFFSET(1)] AS col0_percentile
FROM foo.bar a
