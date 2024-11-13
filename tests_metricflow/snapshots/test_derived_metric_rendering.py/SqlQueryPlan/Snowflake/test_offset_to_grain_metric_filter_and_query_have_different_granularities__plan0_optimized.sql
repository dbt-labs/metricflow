test_name: test_offset_to_grain_metric_filter_and_query_have_different_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset to grain metric is queried with one granularity and filtered by a different one.
sql_engine: Snowflake
---
-- Read From CTE For node_id=cm_5
WITH cm_4_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__month']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__month
    , SUM(bookings) AS bookings_start_of_month
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_12.ds AS metric_time__day
      , DATE_TRUNC('month', subq_12.ds) AS metric_time__month
      , subq_10.bookings AS bookings
    FROM ***************************.mf_time_spine subq_12
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_10
    ON
      DATE_TRUNC('month', subq_12.ds) = subq_10.metric_time__day
    WHERE DATE_TRUNC('month', subq_12.ds) = subq_12.ds
  ) subq_13
  WHERE metric_time__day = '2020-01-01'
  GROUP BY
    metric_time__month
)

, cm_5_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__month
    , bookings_start_of_month AS bookings_at_start_of_month
  FROM (
    -- Read From CTE For node_id=cm_4
    SELECT
      metric_time__month
      , bookings_start_of_month
    FROM cm_4_cte cm_4_cte
  ) subq_17
)

SELECT
  metric_time__month AS metric_time__month
  , bookings_at_start_of_month AS bookings_at_start_of_month
FROM cm_5_cte cm_5_cte
