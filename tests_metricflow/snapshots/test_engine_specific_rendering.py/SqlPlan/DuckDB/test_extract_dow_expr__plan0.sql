test_name: test_extract_dow_expr
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of EXTRACT with day-of-week date part, which requires normalization on some engines.
sql_engine: DuckDB
---
-- Test Extract DOW Expression
SELECT
  EXTRACT(isodow FROM a.ds) AS dow
FROM foo.bar a
