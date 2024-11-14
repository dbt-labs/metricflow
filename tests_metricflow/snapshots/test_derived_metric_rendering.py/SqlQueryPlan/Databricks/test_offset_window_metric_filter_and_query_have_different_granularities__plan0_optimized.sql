test_name: test_offset_window_metric_filter_and_query_have_different_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset window metric is queried with one granularity and filtered by a different one.
sql_engine: Databricks
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['booking_value', 'metric_time__month']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__month
    , SUM(booking_value) AS booking_value
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_19.ds AS metric_time__day
      , DATE_TRUNC('month', subq_19.ds) AS metric_time__month
      , bookings_source_src_28000.booking_value AS booking_value
    FROM ***************************.mf_time_spine subq_19
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      DATEADD(week, -1, subq_19.ds) = DATE_TRUNC('day', bookings_source_src_28000.ds)
  ) subq_20
  WHERE metric_time__day = '2020-01-01'
  GROUP BY
    metric_time__month
)

, cm_7_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookers', 'metric_time__month']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__month
    , COUNT(DISTINCT bookers) AS bookers
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , DATE_TRUNC('month', ds) AS metric_time__month
      , guest_id AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_26
  WHERE metric_time__day = '2020-01-01'
  GROUP BY
    metric_time__month
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__month
    , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_6_cte.metric_time__month, cm_7_cte.metric_time__month) AS metric_time__month
      , MAX(cm_6_cte.booking_value) AS booking_value
      , MAX(cm_7_cte.bookers) AS bookers
    FROM cm_6_cte cm_6_cte
    FULL OUTER JOIN
      cm_7_cte cm_7_cte
    ON
      cm_6_cte.metric_time__month = cm_7_cte.metric_time__month
    GROUP BY
      COALESCE(cm_6_cte.metric_time__month, cm_7_cte.metric_time__month)
  ) subq_31
)

SELECT
  metric_time__month AS metric_time__month
  , booking_fees_last_week_per_booker_this_week AS booking_fees_last_week_per_booker_this_week
FROM cm_8_cte cm_8_cte
