test_name: test_component_rendering
test_filename: test_sql_plan_render.py
docstring:
  Checks that all components of SELECT query are rendered for the 0, 1, >1 component count cases.
sql_engine: Databricks
---
-- test0
SELECT
  SUM(1) AS bookings
FROM demo.fct_bookings a
