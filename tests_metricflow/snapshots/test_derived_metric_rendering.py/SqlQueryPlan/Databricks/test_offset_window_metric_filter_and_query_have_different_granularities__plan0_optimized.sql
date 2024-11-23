test_name: test_offset_window_metric_filter_and_query_have_different_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset window metric is queried with one granularity and filtered by a different one.
sql_engine: Databricks
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , DATE_TRUNC('month', ds) AS metric_time__month
    , booking_value
    , guest_id AS bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__month AS metric_time__month
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_26.metric_time__month, subq_31.metric_time__month) AS metric_time__month
    , MAX(subq_26.booking_value) AS booking_value
    , MAX(subq_31.bookers) AS bookers
  FROM (
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
        time_spine_src_28006.ds AS metric_time__day
        , DATE_TRUNC('month', time_spine_src_28006.ds) AS metric_time__month
        , sma_28009_cte.booking_value AS booking_value
      FROM ***************************.mf_time_spine time_spine_src_28006
      INNER JOIN
        sma_28009_cte sma_28009_cte
      ON
        DATEADD(week, -1, time_spine_src_28006.ds) = sma_28009_cte.metric_time__day
    ) subq_22
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) subq_26
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookers', 'metric_time__month']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__month
      , COUNT(DISTINCT bookers) AS bookers
    FROM (
      -- Read From CTE For node_id=sma_28009
      SELECT
        metric_time__day
        , metric_time__month
        , booking_value
        , bookers
      FROM sma_28009_cte sma_28009_cte
    ) subq_27
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) subq_31
  ON
    subq_26.metric_time__month = subq_31.metric_time__month
  GROUP BY
    COALESCE(subq_26.metric_time__month, subq_31.metric_time__month)
) subq_32
