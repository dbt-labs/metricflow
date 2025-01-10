test_name: test_component_rendering
test_filename: test_sql_plan_render.py
docstring:
  Checks that all components of SELECT query are rendered for the 0, 1, >1 component count cases.
sql_engine: DuckDB
---
-- test2
SELECT
  SUM(1) AS bookings
  , b.country AS user__country
  , c.country AS listing__country
FROM demo.fct_bookings a
LEFT OUTER JOIN
  demo.dim_users b
ON
  a.user_id = b.user_id