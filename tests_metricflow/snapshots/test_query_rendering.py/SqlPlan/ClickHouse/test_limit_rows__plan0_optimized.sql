test_name: test_limit_rows
test_filename: test_query_rendering.py
docstring:
  Tests a plan with a limit to the number of rows returned.
sql_engine: ClickHouse
---
SELECT
  ds__day
  , SUM(__bookings) AS bookings
FROM (
  SELECT
    toStartOfDay(ds) AS ds__day
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_10
GROUP BY
  ds__day
LIMIT 1
