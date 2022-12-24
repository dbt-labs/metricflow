-- Join to Time Spine Dataset
SELECT
  time_spine_alias.metric_time AS metric_time
  , parent_alias.listing AS listing
  , parent_alias.booking_fees AS booking_fees
FROM (
  -- Date Spine
  SELECT
    ds AS metric_time
  FROM ***************************.mf_time_spine subq_11
  WHERE ds BETWEEN CAST('2020-01-01' AS TIMESTAMP) AND CAST('2021-01-01' AS TIMESTAMP)
) subq_11
LEFT OUTER JOIN (
  -- Read Elements From Data Source 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['booking_value', 'metric_time', 'listing']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    ds AS metric_time
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10001
  GROUP BY
    ds
    , listing_id
) subq_10
ON
  subq_11.metric_time = subq_10.metric_time
