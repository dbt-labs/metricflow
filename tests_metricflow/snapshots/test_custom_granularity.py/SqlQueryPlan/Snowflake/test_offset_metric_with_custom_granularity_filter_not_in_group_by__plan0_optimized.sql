test_name: test_offset_metric_with_custom_granularity_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: Snowflake
---
-- Read From CTE For node_id=cm_5
WITH cm_4_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings_5_days_ago
  FROM (
    -- Join to Time Spine Dataset
    -- Join to Custom Granularity Dataset
    SELECT
      subq_13.ds AS metric_time__day
      , subq_11.bookings AS bookings
      , subq_14.martian_day AS metric_time__martian_day
    FROM ***************************.mf_time_spine subq_13
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_11
    ON
      DATEADD(day, -5, subq_13.ds) = subq_11.metric_time__day
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_14
    ON
      subq_13.ds = subq_14.ds
  ) subq_15
  WHERE metric_time__martian_day = '2020-01-01'
  GROUP BY
    metric_time__day
)

, cm_5_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , bookings_5_days_ago AS bookings_5_day_lag
  FROM (
    -- Read From CTE For node_id=cm_4
    SELECT
      metric_time__day
      , bookings_5_days_ago
    FROM cm_4_cte cm_4_cte
  ) subq_19
)

SELECT
  metric_time__day AS metric_time__day
  , bookings_5_day_lag AS bookings_5_day_lag
FROM cm_5_cte cm_5_cte
