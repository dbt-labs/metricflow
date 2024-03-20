-- Join Self Over Time Range
-- Pass Only Elements: ['bookings_monthly', 'metric_time__month']
-- Constrain Time Range to [2020-03-05T00:00:00, 2021-01-04T00:00:00]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  subq_14.metric_time__month AS metric_time__month
  , SUM(subq_13.bookings_monthly) AS trailing_3_months_bookings
FROM (
  -- Time Spine
  SELECT
    DATE_TRUNC('month', ds) AS metric_time__month
  FROM ***************************.mf_time_spine subq_15
  WHERE ds BETWEEN '2020-03-05' AND '2021-01-04'
  GROUP BY
    DATE_TRUNC('month', ds)
) subq_14
INNER JOIN (
  -- Read Elements From Semantic Model 'bookings_monthly_source'
  -- Metric Time Dimension 'monthly_ds'
  -- Constrain Time Range to [2019-12-05T00:00:00, 2021-01-04T00:00:00]
  SELECT
    DATE_TRUNC('month', ds) AS metric_time__month
    , bookings_monthly
  FROM ***************************.fct_bookings_extended_monthly bookings_monthly_source_src_16000
  WHERE DATE_TRUNC('month', ds) BETWEEN '2019-12-05' AND '2021-01-04'
) subq_13
ON
  (
    subq_13.metric_time__month <= subq_14.metric_time__month
  ) AND (
    subq_13.metric_time__month > subq_14.metric_time__month - INTERVAL 3 month
  )
WHERE subq_14.metric_time__month BETWEEN '2020-03-05' AND '2021-01-04'
GROUP BY
  subq_14.metric_time__month
