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
  -- Pass Only Elements: ['bookers', 'listing__country_latest', 'booking__is_instant', 'metric_time__day']
  SELECT
    subq_19.metric_time__day AS metric_time__day
    , subq_19.booking__is_instant AS booking__is_instant
    , listings_latest_src_28000.country AS listing__country_latest
    , subq_19.bookers AS bookers
  FROM (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['bookers', 'booking__is_instant', 'metric_time__day', 'listing']
    SELECT
      subq_17.ds AS metric_time__day
      , bookings_source_src_28000.listing_id AS listing
      , bookings_source_src_28000.is_instant AS booking__is_instant
      , bookings_source_src_28000.guest_id AS bookers
    FROM ***************************.mf_time_spine subq_17
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_17.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_28000.ds) > subq_17.ds - MAKE_INTERVAL(days => 2)
      )
  ) subq_19
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    subq_19.listing = listings_latest_src_28000.listing_id
) subq_24
WHERE booking__is_instant
GROUP BY
  metric_time__day
  , listing__country_latest
