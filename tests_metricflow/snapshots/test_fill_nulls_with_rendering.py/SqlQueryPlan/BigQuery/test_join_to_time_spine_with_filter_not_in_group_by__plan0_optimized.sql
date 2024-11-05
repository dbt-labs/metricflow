-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  subq_14.metric_time__day AS metric_time__day
  , subq_13.bookings AS bookings_join_to_time_spine_with_tiered_filters
FROM (
  -- Filter Time Spine
  SELECT
    metric_time__day
  FROM (
    -- Time Spine
    SELECT
      ds AS metric_time__day
      , DATETIME_TRUNC(ds, month) AS metric_time__month
    FROM ***************************.mf_time_spine subq_15
  ) subq_16
  WHERE (
    metric_time__day <= '2020-01-02'
  ) AND (
    metric_time__month > '2020-01-01'
  )
) subq_14
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
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , DATETIME_TRUNC(ds, month) AS metric_time__month
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_10
  WHERE ((metric_time__day >= '2020-01-02') AND (metric_time__day <= '2020-01-02')) AND (metric_time__month > '2020-01-01')
  GROUP BY
    metric_time__day
) subq_13
ON
  subq_14.metric_time__day = subq_13.metric_time__day
