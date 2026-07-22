test_name: test_generate_uuid
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the generate uuid expression in a query.
sql_engine: Vertica
---
-- Test Generate UUID Expression
SELECT
  UUID_GENERATE() AS uuid
FROM foo.bar a
