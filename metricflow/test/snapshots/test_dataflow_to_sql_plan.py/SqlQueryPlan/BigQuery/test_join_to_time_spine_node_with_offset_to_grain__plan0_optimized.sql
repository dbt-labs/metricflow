-- Join to Time Spine Dataset
SELECT
  subq_12.metric_time__day AS metric_time__day
  , subq_11.listing AS listing
  , subq_11.booking_fees AS booking_fees
FROM (
  -- Date Spine
  SELECT
    ds AS metric_time__day
  FROM ***************************.mf_time_spine subq_13
  WHERE ds BETWEEN '2020-01-01' AND '2021-01-01'
) subq_12
INNER JOIN (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , listing
    , booking_value * 0.05 AS booking_fees
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['booking_value', 'metric_time__day', 'listing']
    -- Aggregate Measures
    SELECT
      DATE_TRUNC(ds, day) AS metric_time__day
      , listing_id AS listing
      , SUM(booking_value) AS booking_value
    FROM ***************************.fct_bookings bookings_source_src_10001
    GROUP BY
      metric_time__day
      , listing
  ) subq_10
) subq_11
ON
  DATE_TRUNC(subq_12.metric_time__day, month) = subq_11.metric_time__day
