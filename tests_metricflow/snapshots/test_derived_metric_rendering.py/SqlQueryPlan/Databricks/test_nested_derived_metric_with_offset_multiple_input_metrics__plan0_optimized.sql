test_name: test_nested_derived_metric_with_offset_multiple_input_metrics
test_filename: test_derived_metric_rendering.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , booking_fees - booking_fees_start_of_month AS booking_fees_since_start_of_month
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_24.metric_time__day, subq_29.metric_time__day) AS metric_time__day
    , MAX(subq_24.booking_fees_start_of_month) AS booking_fees_start_of_month
    , MAX(subq_29.booking_fees) AS booking_fees
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_23.ds AS metric_time__day
      , subq_21.booking_fees_start_of_month AS booking_fees_start_of_month
    FROM ***************************.mf_time_spine subq_23
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , booking_value * 0.05 AS booking_fees_start_of_month
      FROM (
        -- Read From CTE For node_id=sma_28009
        -- Pass Only Elements: ['booking_value', 'metric_time__day']
        -- Aggregate Measures
        -- Compute Metrics via Expressions
        SELECT
          metric_time__day
          , SUM(booking_value) AS booking_value
        FROM sma_28009_cte sma_28009_cte
        GROUP BY
          metric_time__day
      ) subq_20
    ) subq_21
    ON
      DATE_TRUNC('month', subq_23.ds) = subq_21.metric_time__day
  ) subq_24
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , booking_value * 0.05 AS booking_fees
    FROM (
      -- Read From CTE For node_id=sma_28009
      -- Pass Only Elements: ['booking_value', 'metric_time__day']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , SUM(booking_value) AS booking_value
      FROM sma_28009_cte sma_28009_cte
      GROUP BY
        metric_time__day
    ) subq_28
  ) subq_29
  ON
    subq_24.metric_time__day = subq_29.metric_time__day
  GROUP BY
    COALESCE(subq_24.metric_time__day, subq_29.metric_time__day)
) subq_30
