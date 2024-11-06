-- Constrain Output with WHERE
-- Pass Only Elements: ['bookers', 'listing__country_latest', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , listing__country_latest
  , COUNT(DISTINCT bookers) AS every_two_days_bookers
FROM (
  -- Join Standard Outputs
  SELECT
    listings_latest_src_28000.country AS listing__country_latest
    , subq_16.metric_time__day AS metric_time__day
    , subq_16.booking__is_instant AS booking__is_instant
    , subq_16.bookers AS bookers
  FROM (
    -- Join Self Over Time Range
    SELECT
      subq_15.ds AS metric_time__day
      , bookings_source_src_28000.listing_id AS listing
      , bookings_source_src_28000.is_instant AS booking__is_instant
      , bookings_source_src_28000.guest_id AS bookers
    FROM ***************************.mf_time_spine subq_15
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_15.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_28000.ds) > DATEADD(day, -2, subq_15.ds)
      )
  ) subq_16
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    subq_16.listing = listings_latest_src_28000.listing_id
) subq_20
WHERE booking__is_instant
GROUP BY
  metric_time__day
  , listing__country_latest
