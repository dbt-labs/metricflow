-- Join Standard Outputs
-- Pass Only Elements: ['bookers', 'listing__country_latest', 'booking__is_instant', 'metric_time__day']
-- Pass Only Elements: ['bookers', 'listing__country_latest', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_20.metric_time__day AS metric_time__day
  , listings_latest_src_28000.country AS listing__country_latest
  , COUNT(DISTINCT subq_20.bookers) AS every_two_days_bookers
FROM (
  -- Join Self Over Time Range
  -- Pass Only Elements: ['bookers', 'booking__is_instant', 'metric_time__day', 'listing']
  SELECT
    subq_18.ds AS metric_time__day
    , subq_16.listing AS listing
    , subq_16.bookers AS bookers
  FROM ***************************.mf_time_spine subq_18
  INNER JOIN (
    -- Constrain Output with WHERE
    SELECT
      metric_time__day
      , listing
      , bookers
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , listing_id AS listing
        , is_instant AS booking__is_instant
        , guest_id AS bookers
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_15
    WHERE booking__is_instant
  ) subq_16
  ON
    (
      subq_16.metric_time__day <= subq_18.ds
    ) AND (
      subq_16.metric_time__day > DATE_SUB(CAST(subq_18.ds AS DATETIME), INTERVAL 2 day)
    )
) subq_20
LEFT OUTER JOIN
  ***************************.dim_listings_latest listings_latest_src_28000
ON
  subq_20.listing = listings_latest_src_28000.listing_id
GROUP BY
  metric_time__day
  , listing__country_latest
