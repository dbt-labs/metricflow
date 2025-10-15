test_name: test_derived_metric_with_offset_window_and_offset_to_grain
test_filename: test_derived_metric_rendering.py
sql_engine: Postgres
---
-- Compute Metrics via Expressions
-- Write to DataTable
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
    -- Compute Metrics via Expressions
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_22.month_start_bookings AS month_start_bookings
    FROM rss_28018_cte
    INNER JOIN (
      -- Read From CTE For node_id=sma_28009
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        metric_time__day
        , SUM(bookings) AS month_start_bookings
      FROM sma_28009_cte
      GROUP BY
        metric_time__day
    ) subq_22
    ON
      DATE_TRUNC('month', rss_28018_cte.ds__day) = subq_22.metric_time__day
  ) subq_27
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Compute Metrics via Expressions
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_30.bookings_1_month_ago AS bookings_1_month_ago
    FROM rss_28018_cte
    INNER JOIN (
      -- Read From CTE For node_id=sma_28009
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        metric_time__day
        , SUM(bookings) AS bookings_1_month_ago
      FROM sma_28009_cte
      GROUP BY
        metric_time__day
    ) subq_30
    ON
      rss_28018_cte.ds__day - MAKE_INTERVAL(months => 1) = subq_30.metric_time__day
  ) subq_35
  ON
    subq_27.metric_time__day = subq_35.metric_time__day
  GROUP BY
    COALESCE(subq_27.metric_time__day, subq_35.metric_time__day)
) subq_36
