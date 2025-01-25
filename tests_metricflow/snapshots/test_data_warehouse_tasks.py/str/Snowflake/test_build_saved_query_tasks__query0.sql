test_name: test_build_saved_query_tasks
test_filename: test_data_warehouse_tasks.py
sql_engine: Snowflake
---
SELECT
  metric_time__day
  , listing__capacity_latest
  , SUM(bookings) AS bookings
  , SUM(instant_bookings) AS instant_bookings
FROM (
  SELECT
    listings_latest_src_10000.capacity AS listing__capacity_latest
    , nr_subq_1.metric_time__day AS metric_time__day
    , nr_subq_1.bookings AS bookings
    , nr_subq_1.instant_bookings AS instant_bookings
  FROM (
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS bookings
      , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
    FROM ***************************.fct_bookings bookings_source_src_10000
  ) nr_subq_1
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10000
  ON
    nr_subq_1.listing = listings_latest_src_10000.listing_id
) nr_subq_5
WHERE listing__capacity_latest > 3
GROUP BY
  metric_time__day
  , listing__capacity_latest
