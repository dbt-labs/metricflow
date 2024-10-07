-- Compute Metrics via Expressions
SELECT
  metric_time__month
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_9.metric_time__month AS metric_time__month
    , subq_8.bookings AS bookings
  FROM (
    -- Time Spine
    SELECT
      DATE_TRUNC('month', ds) AS metric_time__month
    FROM ***************************.mf_time_spine subq_10
    GROUP BY
      DATE_TRUNC('month', ds)
  ) subq_9
  LEFT OUTER JOIN (
    -- Aggregate Measures
    SELECT
      metric_time__month
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__month']
      SELECT
        DATE_TRUNC('month', ds) AS metric_time__month
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_7
    GROUP BY
      metric_time__month
  ) subq_8
  ON
    subq_9.metric_time__month = subq_8.metric_time__month
) subq_11
