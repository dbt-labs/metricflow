test_name: test_discrete_percentile_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the discrete percentile expression in a query.
sql_engine: DuckDB
---
-- Test Discrete Percentile Expression
SELECT
  PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY (a.col0)) AS col0_percentile
FROM foo.bar a
