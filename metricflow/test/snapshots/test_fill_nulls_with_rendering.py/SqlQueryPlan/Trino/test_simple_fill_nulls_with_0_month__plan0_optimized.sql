-- Compute Metrics via Expressions
SELECT
  metric_time__month
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_11.metric_time__month AS metric_time__month
    , subq_10.bookings AS bookings
  FROM (
    -- Date Spine
    SELECT
      DATE_TRUNC('month', ds) AS metric_time__month
    FROM ***************************.mf_time_spine subq_12
    GROUP BY
      DATE_TRUNC('month', ds)
  ) subq_11
  LEFT OUTER JOIN (
    -- Aggregate Measures
    SELECT
      metric_time__month
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['bookings', 'metric_time__month']
      SELECT
        DATE_TRUNC('month', ds) AS metric_time__month
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_9
    GROUP BY
      metric_time__month
  ) subq_10
  ON
    subq_11.metric_time__month = subq_10.metric_time__month
) subq_13
