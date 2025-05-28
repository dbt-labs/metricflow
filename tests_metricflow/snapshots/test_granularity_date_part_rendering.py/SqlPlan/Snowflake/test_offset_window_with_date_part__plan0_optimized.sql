test_name: test_offset_window_with_date_part
test_filename: test_granularity_date_part_rendering.py
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , EXTRACT(dayofweekiso FROM ds) AS metric_time__extract_dow
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__extract_dow AS metric_time__extract_dow
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_19.metric_time__extract_dow, subq_27.metric_time__extract_dow) AS metric_time__extract_dow
    , MAX(subq_19.bookings) AS bookings
    , MAX(subq_27.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['bookings', 'metric_time__extract_dow']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__extract_dow
      , SUM(bookings) AS bookings
    FROM sma_28009_cte
    GROUP BY
      metric_time__extract_dow
  ) subq_19
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__extract_dow']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      EXTRACT(dayofweekiso FROM time_spine_src_28006.ds) AS metric_time__extract_dow
      , SUM(sma_28009_cte.bookings) AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine time_spine_src_28006
    INNER JOIN
      sma_28009_cte
    ON
      DATEADD(day, -14, time_spine_src_28006.ds) = sma_28009_cte.metric_time__day
    GROUP BY
      EXTRACT(dayofweekiso FROM time_spine_src_28006.ds)
  ) subq_27
  ON
    subq_19.metric_time__extract_dow = subq_27.metric_time__extract_dow
  GROUP BY
    COALESCE(subq_19.metric_time__extract_dow, subq_27.metric_time__extract_dow)
) subq_28
