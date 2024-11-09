test_name: test_generate_uuid
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the generate uuid expression in a query.
sql_engine: Postgres
---
-- Test Generate UUID Expression
SELECT
  GEN_RANDOM_UUID() AS uuid
FROM foo.bar a
