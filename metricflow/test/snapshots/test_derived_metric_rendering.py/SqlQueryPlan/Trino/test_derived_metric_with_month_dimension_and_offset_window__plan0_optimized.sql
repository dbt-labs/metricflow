-- Compute Metrics via Expressions
SELECT
  metric_time__month
  , bookings_last_month AS bookings_last_month
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements:
  --   ['bookings_monthly', 'metric_time__month']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_10.metric_time__month AS metric_time__month
    , SUM(bookings_monthly_source_src_10026.bookings_monthly) AS bookings_last_month
  FROM (
    -- Date Spine
    SELECT
      DATE_TRUNC('month', ds) AS metric_time__month
    FROM ***************************.mf_time_spine subq_11
    GROUP BY
      DATE_TRUNC('month', ds)
  ) subq_10
  INNER JOIN
    ***************************.fct_bookings_extended_monthly bookings_monthly_source_src_10026
  ON
    DATE_ADD('month', -1, subq_10.metric_time__month) = DATE_TRUNC('month', bookings_monthly_source_src_10026.ds)
  GROUP BY
    subq_10.metric_time__month
) subq_15
