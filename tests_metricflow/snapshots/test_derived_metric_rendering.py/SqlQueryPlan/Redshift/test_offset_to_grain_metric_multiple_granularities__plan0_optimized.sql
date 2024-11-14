test_name: test_offset_to_grain_metric_multiple_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset to grain metric is queried with multiple granularities.
sql_engine: Redshift
---
-- Read From CTE For node_id=cm_5
WITH cm_4_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__day', 'metric_time__month', 'metric_time__year']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_11.ds AS metric_time__day
    , DATE_TRUNC('month', subq_11.ds) AS metric_time__month
    , DATE_TRUNC('year', subq_11.ds) AS metric_time__year
    , SUM(subq_9.bookings) AS bookings_start_of_month
  FROM ***************************.mf_time_spine subq_11
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_9
  ON
    DATE_TRUNC('month', subq_11.ds) = subq_9.metric_time__day
  GROUP BY
    subq_11.ds
    , DATE_TRUNC('month', subq_11.ds)
    , DATE_TRUNC('year', subq_11.ds)
)

, cm_5_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , metric_time__month
    , metric_time__year
    , bookings_start_of_month AS bookings_at_start_of_month
  FROM (
    -- Read From CTE For node_id=cm_4
    SELECT
      metric_time__day
      , metric_time__month
      , metric_time__year
      , bookings_start_of_month
    FROM cm_4_cte cm_4_cte
  ) subq_15
)

SELECT
  metric_time__day AS metric_time__day
  , metric_time__month AS metric_time__month
  , metric_time__year AS metric_time__year
  , bookings_at_start_of_month AS bookings_at_start_of_month
FROM cm_5_cte cm_5_cte
