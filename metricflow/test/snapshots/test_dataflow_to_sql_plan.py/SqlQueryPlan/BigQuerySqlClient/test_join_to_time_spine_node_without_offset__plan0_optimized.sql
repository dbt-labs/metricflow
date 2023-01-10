-- Join to Time Spine Dataset
SELECT
  subq_12.metric_time AS metric_time
  , subq_11.listing AS listing
  , subq_11.booking_fees AS booking_fees
FROM (
  -- Date Spine
  SELECT
    ds AS metric_time
  FROM ***************************.mf_time_spine subq_13
  WHERE ds BETWEEN CAST('2020-01-01' AS DATETIME) AND CAST('2021-01-01' AS DATETIME)
) subq_12
LEFT OUTER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    metric_time
    , listing
    , booking_value * 0.05 AS booking_fees
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['booking_value', 'metric_time', 'listing']
    -- Aggregate Measures
    SELECT
      ds AS metric_time
      , listing_id AS listing
      , SUM(booking_value) AS booking_value
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10001
    GROUP BY
      metric_time
      , listing
  ) subq_10
) subq_11
ON
  subq_12.metric_time = subq_11.metric_time
