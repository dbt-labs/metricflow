test_name: test_multiple_metrics_no_dimensions
test_filename: test_query_rendering.py
sql_engine: ClickHouse
---
SELECT
  MAX(subq_25.bookings) AS bookings
  , MAX(subq_32.listings) AS listings
FROM (
  SELECT
    SUM(__bookings) AS bookings
  FROM (
    SELECT
      1 AS __bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
    WHERE toStartOfDay(ds) BETWEEN '2020-01-01' AND '2020-01-01'
  ) subq_23
) subq_25
CROSS JOIN (
  SELECT
    SUM(__listings) AS listings
  FROM (
    SELECT
      1 AS __listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
    WHERE toStartOfDay(created_at) BETWEEN '2020-01-01' AND '2020-01-01'
  ) subq_30
) subq_32
