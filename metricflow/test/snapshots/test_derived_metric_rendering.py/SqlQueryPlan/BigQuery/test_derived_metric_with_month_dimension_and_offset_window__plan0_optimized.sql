-- Compute Metrics via Expressions
SELECT
  metric_time__month
  , bookings_last_month AS bookings_last_month
FROM (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings_monthly', 'metric_time__month']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_10.metric_time__month AS metric_time__month
    , SUM(bookings_monthly_source_src_10026.bookings_monthly) AS bookings_last_month
  FROM (
    -- Time Spine
    SELECT
      DATE_TRUNC(ds, month) AS metric_time__month
    FROM ***************************.mf_time_spine subq_11
    GROUP BY
      metric_time__month
  ) subq_10
  INNER JOIN
    ***************************.fct_bookings_extended_monthly bookings_monthly_source_src_10026
  ON
    DATE_SUB(CAST(subq_10.metric_time__month AS DATETIME), INTERVAL 1 month) = DATE_TRUNC(bookings_monthly_source_src_10026.ds, month)
  GROUP BY
    metric_time__month
) subq_15
