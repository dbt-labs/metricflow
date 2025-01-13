test_name: test_render_where
test_filename: test_sql_plan_render.py
sql_engine: Clickhouse
---
-- test0
SELECT
  a.booking_value
FROM demo.fct_bookings a
WHERE a.booking_value > 100
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
