test_name: test_derived_metric_with_offset_to_grain
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , bookings - bookings_at_start_of_month AS bookings_growth_since_start_of_month
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_18.metric_time__day, subq_25.metric_time__day) AS metric_time__day
    , MAX(subq_18.bookings) AS bookings
    , MAX(subq_25.bookings_at_start_of_month) AS bookings_at_start_of_month
  FROM (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM sma_28009_cte sma_28009_cte
    GROUP BY
      metric_time__day
  ) subq_18
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_21.ds AS metric_time__day
      , SUM(sma_28009_cte.bookings) AS bookings_at_start_of_month
    FROM ***************************.mf_time_spine subq_21
    INNER JOIN
      sma_28009_cte sma_28009_cte
    ON
      DATETIME_TRUNC(subq_21.ds, month) = sma_28009_cte.metric_time__day
    GROUP BY
      metric_time__day
  ) subq_25
  ON
    subq_18.metric_time__day = subq_25.metric_time__day
  GROUP BY
    metric_time__day
) subq_26
