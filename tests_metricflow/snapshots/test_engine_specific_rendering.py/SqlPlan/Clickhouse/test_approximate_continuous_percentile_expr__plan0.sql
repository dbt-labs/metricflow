test_name: test_approximate_continuous_percentile_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the approximate continuous percentile expression in a query.
sql_engine: Clickhouse
---
-- Test Approximate Continuous Percentile Expression
SELECT
  quantile(0.5)(a.col0) AS col0_percentile
FROM foo.bar a
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
