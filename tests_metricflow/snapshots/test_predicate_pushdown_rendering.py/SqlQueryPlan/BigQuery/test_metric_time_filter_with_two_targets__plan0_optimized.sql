-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'listing__country_latest']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  listing__country_latest
  , SUM(bookings) AS bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['bookings', 'listing__country_latest', 'metric_time__day']
  SELECT
    subq_13.metric_time__day AS metric_time__day
    , listings_latest_src_28000.country AS listing__country_latest
    , subq_13.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'metric_time__day', 'listing']
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_13
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    subq_13.listing = listings_latest_src_28000.listing_id
) subq_18
WHERE metric_time__day = '2024-01-01'
GROUP BY
  listing__country_latest
