test_name: test_approximate_discrete_percentile_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the approximate discrete percentile expression in a query.
sql_engine: ClickHouse
---
SELECT
  quantileTDigest(0.5)(a.col0) AS col0_percentile
FROM foo.bar a
SETTINGS join_use_nulls = 1
