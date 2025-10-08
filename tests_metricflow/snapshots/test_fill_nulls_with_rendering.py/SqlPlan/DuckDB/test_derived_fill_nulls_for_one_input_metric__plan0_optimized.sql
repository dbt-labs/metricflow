test_name: test_derived_fill_nulls_for_one_input_metric
test_filename: test_fill_nulls_with_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0_for_non_offset
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_19.metric_time__day, subq_27.metric_time__day) AS metric_time__day
    , MAX(subq_19.bookings_fill_nulls_with_0) AS bookings_fill_nulls_with_0
    , MAX(subq_27.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings_fill_nulls_with_0
    FROM sma_28009_cte
    GROUP BY
      metric_time__day
  ) subq_19
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Compute Metrics via Expressions
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , subq_22.bookings AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN (
      -- Read From CTE For node_id=sma_28009
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      -- Aggregate Measures
      SELECT
        metric_time__day
        , SUM(bookings) AS bookings
      FROM sma_28009_cte
      GROUP BY
        metric_time__day
    ) subq_22
    ON
      time_spine_src_28006.ds - INTERVAL 14 day = subq_22.metric_time__day
  ) subq_27
  ON
    subq_19.metric_time__day = subq_27.metric_time__day
  GROUP BY
    COALESCE(subq_19.metric_time__day, subq_27.metric_time__day)
) subq_28
