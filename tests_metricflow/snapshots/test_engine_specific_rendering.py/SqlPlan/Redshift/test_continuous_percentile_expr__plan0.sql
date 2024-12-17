test_name: test_continuous_percentile_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the continuous percentile expression in a query.
sql_engine: Redshift
---
-- Test Continuous Percentile Expression
SELECT
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (a.col0)) AS col0_percentile
FROM foo.bar a
