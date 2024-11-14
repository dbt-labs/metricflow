test_name: test_nested_derived_metric_with_offset_multiple_input_metrics
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Read From CTE For node_id=cm_14
WITH cm_10_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['booking_value', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , SUM(booking_value) AS booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    DATE_TRUNC('day', ds)
)

, cm_11_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , booking_value * 0.05 AS booking_fees_start_of_month
  FROM (
    -- Read From CTE For node_id=cm_10
    SELECT
      metric_time__day
      , booking_value
    FROM cm_10_cte cm_10_cte
  ) subq_20
)

, cm_12_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['booking_value', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , SUM(booking_value) AS booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    DATE_TRUNC('day', ds)
)

, cm_13_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , booking_value * 0.05 AS booking_fees
  FROM (
    -- Read From CTE For node_id=cm_12
    SELECT
      metric_time__day
      , booking_value
    FROM cm_12_cte cm_12_cte
  ) subq_29
)

, cm_14_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , booking_fees - booking_fees_start_of_month AS booking_fees_since_start_of_month
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_24.metric_time__day, cm_13_cte.metric_time__day) AS metric_time__day
      , MAX(subq_24.booking_fees_start_of_month) AS booking_fees_start_of_month
      , MAX(cm_13_cte.booking_fees) AS booking_fees
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_23.ds AS metric_time__day
        , cm_11_cte.booking_fees_start_of_month AS booking_fees_start_of_month
      FROM ***************************.mf_time_spine subq_23
      INNER JOIN
        cm_11_cte cm_11_cte
      ON
        DATE_TRUNC('month', subq_23.ds) = cm_11_cte.metric_time__day
    ) subq_24
    FULL OUTER JOIN
      cm_13_cte cm_13_cte
    ON
      subq_24.metric_time__day = cm_13_cte.metric_time__day
    GROUP BY
      COALESCE(subq_24.metric_time__day, cm_13_cte.metric_time__day)
  ) subq_31
)

SELECT
  metric_time__day AS metric_time__day
  , booking_fees_since_start_of_month AS booking_fees_since_start_of_month
FROM cm_14_cte cm_14_cte
