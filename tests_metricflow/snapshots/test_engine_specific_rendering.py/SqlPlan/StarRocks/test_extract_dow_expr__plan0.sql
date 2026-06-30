test_name: test_extract_dow_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of EXTRACT with day-of-week date part, which requires normalization on some engines.
sql_engine: StarRocks
---
-- Test Extract DOW Expression
SELECT
  CASE WHEN EXTRACT(DAYOFWEEK FROM a.ds) = 1 THEN 7 ELSE EXTRACT(DAYOFWEEK FROM a.ds) - 1 END AS dow
FROM foo.bar a
