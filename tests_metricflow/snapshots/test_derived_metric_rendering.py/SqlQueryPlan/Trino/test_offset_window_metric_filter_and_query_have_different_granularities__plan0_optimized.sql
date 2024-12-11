test_name: test_offset_window_metric_filter_and_query_have_different_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset window metric is queried with one granularity and filtered by a different one.
sql_engine: Trino
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
    COALESCE(subq_24.metric_time__month, subq_29.metric_time__month) AS metric_time__month
    , MAX(subq_24.booking_value) AS booking_value
    , MAX(subq_29.bookers) AS bookers
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
        subq_19.ds AS metric_time__day
        , DATE_TRUNC('month', subq_19.ds) AS metric_time__month
        , sma_28009_cte.booking_value AS booking_value
      FROM ***************************.mf_time_spine subq_19
      INNER JOIN
        sma_28009_cte sma_28009_cte
      ON
        DATE_ADD('week', -1, subq_19.ds) = sma_28009_cte.metric_time__day
    ) subq_20
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) subq_24
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
    ) subq_25
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) subq_29
  ON
    subq_24.metric_time__month = subq_29.metric_time__month
  GROUP BY
    COALESCE(subq_24.metric_time__month, subq_29.metric_time__month)
) subq_30
