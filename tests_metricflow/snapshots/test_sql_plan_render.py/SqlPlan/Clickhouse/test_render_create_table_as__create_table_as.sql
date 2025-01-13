test_name: test_render_create_table_as
test_filename: test_sql_plan_render.py
sql_engine: Clickhouse
---
CREATE TABLE schema_name.table_name AS (
  -- select_0
  SELECT
    a.bookings
  FROM demo.fct_bookings a
  LIMIT 1
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
)
