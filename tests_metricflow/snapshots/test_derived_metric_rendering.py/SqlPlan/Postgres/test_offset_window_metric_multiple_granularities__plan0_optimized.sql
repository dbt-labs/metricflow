test_name: test_offset_window_metric_multiple_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset window metric is queried with multiple granularities.
sql_engine: Postgres
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , DATE_TRUNC('month', ds) AS metric_time__month
    , DATE_TRUNC('year', ds) AS metric_time__year
    , booking_value
    , guest_id AS bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , metric_time__month AS metric_time__month
  , metric_time__year AS metric_time__year
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_23.metric_time__day, subq_27.metric_time__day) AS metric_time__day
    , COALESCE(subq_23.metric_time__month, subq_27.metric_time__month) AS metric_time__month
    , COALESCE(subq_23.metric_time__year, subq_27.metric_time__year) AS metric_time__year
    , MAX(subq_23.booking_value) AS booking_value
    , MAX(subq_27.bookers) AS bookers
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['booking_value', 'metric_time__day', 'metric_time__month', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , DATE_TRUNC('month', time_spine_src_28006.ds) AS metric_time__month
      , DATE_TRUNC('year', time_spine_src_28006.ds) AS metric_time__year
      , SUM(sma_28009_cte.booking_value) AS booking_value
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN
      sma_28009_cte
    ON
      time_spine_src_28006.ds - MAKE_INTERVAL(weeks => 1) = sma_28009_cte.metric_time__day
    GROUP BY
      time_spine_src_28006.ds
      , DATE_TRUNC('month', time_spine_src_28006.ds)
      , DATE_TRUNC('year', time_spine_src_28006.ds)
  ) subq_23
  FULL OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['bookers', 'metric_time__day', 'metric_time__month', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , metric_time__month
      , metric_time__year
      , COUNT(DISTINCT bookers) AS bookers
    FROM sma_28009_cte
    GROUP BY
      metric_time__day
      , metric_time__month
      , metric_time__year
  ) subq_27
  ON
    (
      subq_23.metric_time__day = subq_27.metric_time__day
    ) AND (
      subq_23.metric_time__month = subq_27.metric_time__month
    ) AND (
      subq_23.metric_time__year = subq_27.metric_time__year
    )
  GROUP BY
    COALESCE(subq_23.metric_time__day, subq_27.metric_time__day)
    , COALESCE(subq_23.metric_time__month, subq_27.metric_time__month)
    , COALESCE(subq_23.metric_time__year, subq_27.metric_time__year)
) subq_28
