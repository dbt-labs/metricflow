test_name: test_generate_uuid
test_filename: test_engine_specific_rendering.py
docstring:
  Tests rendering of the generate uuid expression in a query.
sql_engine: Trino
---
-- Test Generate UUID Expression
SELECT
  uuid() AS uuid
FROM foo.bar a
