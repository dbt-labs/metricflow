-- Constrain Output with WHERE
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , listing__capacity_latest
  , SUM(bookings) AS bookings
  , SUM(instant_bookings) AS instant_bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['bookings', 'instant_bookings', 'listing__capacity_latest', 'metric_time__day']
  SELECT
    subq_2.metric_time__day AS metric_time__day
    , listings_latest_src_10000.capacity AS listing__capacity_latest
    , subq_2.bookings AS bookings
    , subq_2.instant_bookings AS instant_bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'instant_bookings', 'metric_time__day', 'listing']
    SELECT
      DATE_TRUNC(ds, day) AS metric_time__day
      , listing_id AS listing
      , 1 AS bookings
      , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
    FROM ***************************.fct_bookings bookings_source_src_10000
  ) subq_2
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10000
  ON
    subq_2.listing = listings_latest_src_10000.listing_id
) subq_7
WHERE listing__capacity_latest > 3
GROUP BY
  metric_time__day
  , listing__capacity_latest
