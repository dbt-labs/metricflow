test_name: test_derived_metric_with_offset_window_and_offset_to_grain_and_granularity
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__year']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATETIME_TRUNC(subq_20.ds, year) AS metric_time__year
    , SUM(subq_18.bookings) AS month_start_bookings
  FROM ***************************.mf_time_spine subq_20
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_18
  ON
    DATETIME_TRUNC(subq_20.ds, month) = subq_18.metric_time__day
  WHERE DATETIME_TRUNC(subq_20.ds, year) = subq_20.ds
  GROUP BY
    metric_time__year
)

, cm_7_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__year']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATETIME_TRUNC(subq_28.ds, year) AS metric_time__year
    , SUM(subq_26.bookings) AS bookings_1_month_ago
  FROM ***************************.mf_time_spine subq_28
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_26
  ON
    DATE_SUB(CAST(subq_28.ds AS DATETIME), INTERVAL 1 month) = subq_26.metric_time__day
  GROUP BY
    metric_time__year
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__year
    , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_6_cte.metric_time__year, cm_7_cte.metric_time__year) AS metric_time__year
      , MAX(cm_6_cte.month_start_bookings) AS month_start_bookings
      , MAX(cm_7_cte.bookings_1_month_ago) AS bookings_1_month_ago
    FROM cm_6_cte cm_6_cte
    FULL OUTER JOIN
      cm_7_cte cm_7_cte
    ON
      cm_6_cte.metric_time__year = cm_7_cte.metric_time__year
    GROUP BY
      metric_time__year
  ) subq_33
)

SELECT
  metric_time__year AS metric_time__year
  , bookings_month_start_compared_to_1_month_prior AS bookings_month_start_compared_to_1_month_prior
FROM cm_8_cte cm_8_cte
