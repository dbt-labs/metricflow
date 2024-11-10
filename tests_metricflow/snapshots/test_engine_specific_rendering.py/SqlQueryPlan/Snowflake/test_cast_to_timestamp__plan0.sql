test_name: test_cast_to_timestamp
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the cast to timestamp expression in a query.
sql_engine: Snowflake
---
-- Test Cast to Timestamp Expression
SELECT
  CAST('2020-01-01' AS TIMESTAMP) AS col0
FROM foo.bar a
