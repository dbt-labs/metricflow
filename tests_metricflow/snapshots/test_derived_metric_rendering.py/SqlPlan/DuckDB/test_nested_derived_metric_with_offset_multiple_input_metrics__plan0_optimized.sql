test_name: test_nested_derived_metric_with_offset_multiple_input_metrics
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
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
    COALESCE(subq_26.metric_time__day, subq_31.metric_time__day) AS metric_time__day
    , MAX(subq_26.booking_fees_start_of_month) AS booking_fees_start_of_month
    , MAX(subq_31.booking_fees) AS booking_fees
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , subq_22.booking_fees_start_of_month AS booking_fees_start_of_month
    FROM ***************************.mf_time_spine time_spine_src_28006
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
      ) subq_21
    ) subq_22
    ON
      DATE_TRUNC('month', time_spine_src_28006.ds) = subq_22.metric_time__day
  ) subq_26
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
    ) subq_30
  ) subq_31
  ON
    subq_26.metric_time__day = subq_31.metric_time__day
  GROUP BY
    COALESCE(subq_26.metric_time__day, subq_31.metric_time__day)
) subq_32
