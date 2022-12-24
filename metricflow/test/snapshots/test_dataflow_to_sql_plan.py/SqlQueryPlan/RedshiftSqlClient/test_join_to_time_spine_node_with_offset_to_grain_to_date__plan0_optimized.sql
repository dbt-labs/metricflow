-- Join to Time Spine Dataset
SELECT
  subq_9.ds AS ds
  , subq_9.metric_time AS metric_time
  , subq_9.listing AS listing
  , booking_value * 0.05 AS booking_fees
FROM (
  -- Date Spine
  SELECT
    ds AS metric_time
  FROM ***************************.mf_time_spine subq_11
  WHERE ds BETWEEN CAST('2020-01-01' AS TIMESTAMP) AND CAST('2021-01-01' AS TIMESTAMP)
) subq_11
LEFT OUTER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    ds
    , metric_time
    , listing
    , booking_value * 0.05 AS booking_fees
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Pass Only Elements:
    --   ['booking_value', 'ds', 'listing']
    -- Metric Time Dimension 'ds'
    -- Aggregate Measures
    SELECT
      ds
      , ds AS metric_time
      , listing_id AS listing
      , SUM(booking_value) AS booking_value
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10001
    GROUP BY
      ds
      , ds
      , listing_id
  ) subq_9
) subq_10
ON
  DATE_TRUNC('month', subq_11.metric_time) = subq_10.metric_time
