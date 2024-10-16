-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  subq_16.metric_time__day AS metric_time__day
  , subq_15.bookings AS bookings_join_to_time_spine_with_tiered_filters
FROM (
  -- Filter Time Spine
  SELECT
    metric_time__day
  FROM (
    -- Time Spine
    SELECT
      DATE_TRUNC('month', ds) AS booking__ds__month
      , ds AS metric_time__day
    FROM ***************************.mf_time_spine subq_17
  ) subq_18
  WHERE (
    metric_time__day <= '2020-01-02'
  ) AND (
    booking__ds__month > '2020-01-01'
  )
) subq_16
LEFT OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'metric_time__day', 'booking__ds__month']
    SELECT
      DATE_TRUNC('month', ds) AS booking__ds__month
      , DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_12
  WHERE ((metric_time__day >= '2020-01-02') AND (metric_time__day <= '2020-01-02')) AND (booking__ds__month > '2020-01-01')
  GROUP BY
    metric_time__day
) subq_15
ON
  subq_16.metric_time__day = subq_15.metric_time__day
