test_name: test_render_where
test_filename: test_sql_plan_render.py
sql_engine: BigQuery
---
-- test0
SELECT
  a.booking_value
FROM demo.fct_bookings a
WHERE a.booking_value > 100
