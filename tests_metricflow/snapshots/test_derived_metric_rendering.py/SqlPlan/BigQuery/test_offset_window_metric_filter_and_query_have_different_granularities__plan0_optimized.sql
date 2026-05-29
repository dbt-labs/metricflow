test_name: test_offset_window_metric_filter_and_query_have_different_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset window metric is queried with one granularity and filtered by a different one.
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , DATETIME_TRUNC(ds, month) AS metric_time__month
    , booking_value AS __booking_value
    , guest_id AS __bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__month AS metric_time__month
  , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_31.metric_time__month, subq_37.metric_time__month) AS metric_time__month
    , MAX(subq_31.booking_value) AS booking_value
    , MAX(subq_37.bookers) AS bookers
  FROM (
    -- Join to Time Spine Dataset
    -- Select: ['__booking_value', 'metric_time__month']
    -- Select: ['__booking_value', 'metric_time__month']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      subq_26.metric_time__month AS metric_time__month
      , SUM(sma_28009_cte.__booking_value) AS booking_value
    FROM (
      -- Constrain Output with WHERE
      -- Select: ['metric_time__day', 'metric_time__month']
      SELECT
        metric_time__day
        , metric_time__month
      FROM (
        -- Read From Time Spine 'mf_time_spine'
        -- Change Column Aliases
        -- Select: ['metric_time__day', 'metric_time__month']
        SELECT
          ds AS metric_time__day
          , DATETIME_TRUNC(ds, month) AS metric_time__month
        FROM ***************************.mf_time_spine time_spine_src_28006
      ) subq_24
      WHERE metric_time__day = '2020-01-01'
    ) subq_26
    INNER JOIN
      sma_28009_cte
    ON
      DATE_SUB(CAST(subq_26.metric_time__day AS DATETIME), INTERVAL 1 week) = sma_28009_cte.metric_time__day
    GROUP BY
      metric_time__month
  ) subq_31
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Select: ['__bookers', 'metric_time__month']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__month
      , COUNT(DISTINCT bookers) AS bookers
    FROM (
      -- Read From CTE For node_id=sma_28009
      -- Select: ['__bookers', 'metric_time__month', 'metric_time__day']
      SELECT
        metric_time__day
        , metric_time__month
        , __bookers AS bookers
      FROM sma_28009_cte
    ) subq_33
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__month
  ) subq_37
  ON
    subq_31.metric_time__month = subq_37.metric_time__month
  GROUP BY
    metric_time__month
) subq_38
