test_name: test_offset_window_with_date_part
test_filename: test_granularity_date_part_rendering.py
sql_engine: BigQuery
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__extract_dow
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'metric_time__extract_dow']
    SELECT
      IF(EXTRACT(dayofweek FROM ds) = 1, 7, EXTRACT(dayofweek FROM ds) - 1) AS metric_time__extract_dow
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_16
  GROUP BY
    metric_time__extract_dow
)

, cm_7_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__extract_dow']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    IF(EXTRACT(dayofweek FROM subq_22.ds) = 1, 7, EXTRACT(dayofweek FROM subq_22.ds) - 1) AS metric_time__extract_dow
    , SUM(subq_20.bookings) AS bookings_2_weeks_ago
  FROM ***************************.mf_time_spine subq_22
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_20
  ON
    DATE_SUB(CAST(subq_22.ds AS DATETIME), INTERVAL 14 day) = subq_20.metric_time__day
  GROUP BY
    metric_time__extract_dow
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__extract_dow
    , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_6_cte.metric_time__extract_dow, cm_7_cte.metric_time__extract_dow) AS metric_time__extract_dow
      , MAX(cm_6_cte.bookings) AS bookings
      , MAX(cm_7_cte.bookings_2_weeks_ago) AS bookings_2_weeks_ago
    FROM cm_6_cte cm_6_cte
    FULL OUTER JOIN
      cm_7_cte cm_7_cte
    ON
      cm_6_cte.metric_time__extract_dow = cm_7_cte.metric_time__extract_dow
    GROUP BY
      metric_time__extract_dow
  ) subq_27
)

SELECT
  metric_time__extract_dow AS metric_time__extract_dow
  , bookings_growth_2_weeks AS bookings_growth_2_weeks
FROM cm_8_cte cm_8_cte
