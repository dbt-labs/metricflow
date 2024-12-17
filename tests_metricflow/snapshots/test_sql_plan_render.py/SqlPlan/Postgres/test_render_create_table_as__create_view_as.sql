test_name: test_render_create_table_as
test_filename: test_sql_plan_render.py
sql_engine: Postgres
---
CREATE VIEW schema_name.table_name AS (
  -- select_0
  SELECT
    a.bookings
  FROM demo.fct_bookings a
  LIMIT 1
)
