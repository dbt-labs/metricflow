test_name: test_component_rendering
test_filename: test_sql_plan_render.py
docstring:
  Checks that all components of SELECT query are rendered for the 0, 1, >1 component count cases.
sql_engine: Redshift
---
-- test1
SELECT
  SUM(1) AS bookings
  , b.country AS user__country
  , c.country AS listing__country
FROM demo.fct_bookings a
