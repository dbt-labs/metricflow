test_name: test_render_order_by
test_filename: test_sql_plan_render.py
sql_engine: Clickhouse
---
-- test0
SELECT
  a.booking_value
  , a.bookings
FROM demo.fct_bookings a
ORDER BY a.booking_value, a.bookings DESC
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
