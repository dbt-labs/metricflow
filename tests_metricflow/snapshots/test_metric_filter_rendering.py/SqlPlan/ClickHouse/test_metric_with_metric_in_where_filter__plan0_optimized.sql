test_name: test_metric_with_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a metric in the metric-level where filter.
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , SUM(__active_listings) AS active_listings
FROM (
  SELECT
    metric_time__day
    , active_listings AS __active_listings
  FROM (
    SELECT
      subq_24.metric_time__day AS metric_time__day
      , subq_31.listing__bookings AS listing__bookings
      , subq_24.__active_listings AS active_listings
    FROM (
      SELECT
        toStartOfDay(created_at) AS metric_time__day
        , listing_id AS listing
        , 1 AS __active_listings
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_24
    LEFT OUTER JOIN (
      SELECT
        listing
        , SUM(__bookings) AS listing__bookings
      FROM (
        SELECT
          listing_id AS listing
          , 1 AS __bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_28
      GROUP BY
        listing
    ) subq_31
    ON
      subq_24.listing = subq_31.listing
  ) subq_33
  WHERE listing__bookings > 2
) subq_35
GROUP BY
  metric_time__day
