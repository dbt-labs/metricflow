test_name: test_offset_to_grain_metric
test_filename: test_custom_granularity.py
docstring:
  Test a nested offset-to-grain metric with metric time at a custom grain.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , booking_value AS __booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__alien_day AS metric_time__alien_day
  , booking_fees - booking_fees_start_of_month AS booking_fees_since_start_of_month
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_36.metric_time__alien_day, subq_43.metric_time__alien_day) AS metric_time__alien_day
    , MAX(subq_36.booking_fees_start_of_month) AS booking_fees_start_of_month
    , MAX(subq_43.booking_fees) AS booking_fees
  FROM (
    -- Join to Time Spine Dataset
    -- Select: ['metric_time__alien_day', 'booking_fees_start_of_month']
    SELECT
      subq_34.metric_time__alien_day AS metric_time__alien_day
      , subq_30.booking_fees_start_of_month AS booking_fees_start_of_month
    FROM (
      -- Read From Time Spine 'mf_time_spine'
      -- Change Column Aliases
      -- Select: ['metric_time__alien_day']
      -- Select: ['metric_time__alien_day']
      SELECT
        alien_day AS metric_time__alien_day
      FROM ***************************.mf_time_spine time_spine_src_28006
      GROUP BY
        alien_day
    ) subq_34
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__alien_day
        , booking_value * 0.05 AS booking_fees_start_of_month
      FROM (
        -- Read From CTE For node_id=sma_28009
        -- Join to Custom Granularity Dataset
        -- Select: ['__booking_value', 'metric_time__alien_day', 'metric_time__day']
        -- Select: ['__booking_value', 'metric_time__alien_day', 'metric_time__day']
        -- Aggregate Inputs for Simple Metrics
        -- Compute Metrics via Expressions
        SELECT
          subq_24.alien_day AS metric_time__alien_day
          , sma_28009_cte.metric_time__day AS metric_time__day
          , SUM(sma_28009_cte.__booking_value) AS booking_value
        FROM sma_28009_cte
        LEFT OUTER JOIN
          ***************************.mf_time_spine subq_24
        ON
          sma_28009_cte.metric_time__day = subq_24.ds
        GROUP BY
          subq_24.alien_day
          , sma_28009_cte.metric_time__day
      ) subq_29
    ) subq_30
    ON
      DATE_TRUNC('month', subq_34.metric_time__alien_day) = subq_30.metric_time__alien_day
  ) subq_36
  FULL OUTER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__alien_day
      , booking_value * 0.05 AS booking_fees
    FROM (
      -- Read From CTE For node_id=sma_28009
      -- Join to Custom Granularity Dataset
      -- Select: ['__booking_value', 'metric_time__alien_day']
      -- Select: ['__booking_value', 'metric_time__alien_day']
      -- Aggregate Inputs for Simple Metrics
      -- Compute Metrics via Expressions
      SELECT
        subq_37.alien_day AS metric_time__alien_day
        , SUM(sma_28009_cte.__booking_value) AS booking_value
      FROM sma_28009_cte
      LEFT OUTER JOIN
        ***************************.mf_time_spine subq_37
      ON
        sma_28009_cte.metric_time__day = subq_37.ds
      GROUP BY
        subq_37.alien_day
    ) subq_42
  ) subq_43
  ON
    subq_36.metric_time__alien_day = subq_43.metric_time__alien_day
  GROUP BY
    COALESCE(subq_36.metric_time__alien_day, subq_43.metric_time__alien_day)
) subq_44
