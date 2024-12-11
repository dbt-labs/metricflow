test_name: test_offset_window_metric_multiple_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset window metric is queried with multiple granularities.
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , DATETIME_TRUNC(ds, month) AS metric_time__month
    , DATETIME_TRUNC(ds, year) AS metric_time__year
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
    COALESCE(subq_21.metric_time__day, subq_25.metric_time__day) AS metric_time__day
    , COALESCE(subq_21.metric_time__month, subq_25.metric_time__month) AS metric_time__month
    , COALESCE(subq_21.metric_time__year, subq_25.metric_time__year) AS metric_time__year
    , MAX(subq_21.booking_value) AS booking_value
    , MAX(subq_25.bookers) AS bookers
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['booking_value', 'metric_time__day', 'metric_time__month', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_17.ds AS metric_time__day
      , DATETIME_TRUNC(subq_17.ds, month) AS metric_time__month
      , DATETIME_TRUNC(subq_17.ds, year) AS metric_time__year
      , SUM(sma_28009_cte.booking_value) AS booking_value
    FROM ***************************.mf_time_spine subq_17
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      DATE_SUB(CAST(subq_17.ds AS DATETIME), INTERVAL 1 week) = sma_28009_cte.metric_time__day
    GROUP BY
      metric_time__day
      , metric_time__month
      , metric_time__year
  ) subq_21
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
    FROM sma_28009_cte sma_28009_cte
    GROUP BY
      metric_time__day
      , metric_time__month
      , metric_time__year
  ) subq_25
  ON
    (
      subq_21.metric_time__day = subq_25.metric_time__day
    ) AND (
      subq_21.metric_time__month = subq_25.metric_time__month
    ) AND (
      subq_21.metric_time__year = subq_25.metric_time__year
    )
  GROUP BY
    metric_time__day
    , metric_time__month
    , metric_time__year
) subq_26
