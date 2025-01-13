test_name: test_generate_uuid
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the generate uuid expression in a query.
sql_engine: Clickhouse
---
-- Test Generate UUID Expression
SELECT
  generateUUIDv4() AS uuid
FROM foo.bar a
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
