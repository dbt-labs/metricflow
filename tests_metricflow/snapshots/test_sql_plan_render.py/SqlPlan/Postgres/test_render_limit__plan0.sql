test_name: test_render_limit
test_filename: test_sql_plan_render.py
sql_engine: Postgres
---
-- test0
SELECT
  a.bookings
FROM demo.fct_bookings a
LIMIT 1
