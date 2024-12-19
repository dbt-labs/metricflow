test_name: test_derived_metric_with_offset_window_and_offset_to_grain
test_filename: test_derived_metric_rendering.py
sql_engine: Redshift
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

, rss_28018_cte AS (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    ds AS ds__day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__day AS metric_time__day
  , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_27.metric_time__day, subq_35.metric_time__day) AS metric_time__day
    , MAX(subq_27.month_start_bookings) AS month_start_bookings
    , MAX(subq_35.bookings_1_month_ago) AS bookings_1_month_ago
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , SUM(sma_28009_cte.bookings) AS month_start_bookings
    FROM rss_28018_cte rss_28018_cte
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      DATE_TRUNC('month', rss_28018_cte.ds__day) = sma_28009_cte.metric_time__day
    GROUP BY
      rss_28018_cte.ds__day
  ) subq_27
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , SUM(sma_28009_cte.bookings) AS bookings_1_month_ago
    FROM rss_28018_cte rss_28018_cte
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      DATEADD(month, -1, rss_28018_cte.ds__day) = sma_28009_cte.metric_time__day
    GROUP BY
      rss_28018_cte.ds__day
  ) subq_35
  ON
    subq_27.metric_time__day = subq_35.metric_time__day
  GROUP BY
    COALESCE(subq_27.metric_time__day, subq_35.metric_time__day)
) subq_36
