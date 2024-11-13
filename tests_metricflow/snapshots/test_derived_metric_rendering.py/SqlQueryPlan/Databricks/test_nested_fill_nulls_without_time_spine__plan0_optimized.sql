test_name: test_nested_fill_nulls_without_time_spine
test_filename: test_derived_metric_rendering.py
sql_engine: Databricks
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0_without_time_spine
  FROM (
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_8
    GROUP BY
      metric_time__day
  ) subq_9
)

, cm_7_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , 2 * bookings_fill_nulls_with_0_without_time_spine AS twice_bookings_fill_nulls_with_0_without_time_spine
  FROM (
    -- Read From CTE For node_id=cm_6
    SELECT
      metric_time__day
      , bookings_fill_nulls_with_0_without_time_spine
    FROM cm_6_cte cm_6_cte
  ) subq_10
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , 3 * twice_bookings_fill_nulls_with_0_without_time_spine AS nested_fill_nulls_without_time_spine
  FROM (
    -- Read From CTE For node_id=cm_7
    SELECT
      metric_time__day
      , twice_bookings_fill_nulls_with_0_without_time_spine
    FROM cm_7_cte cm_7_cte
  ) subq_11
)

SELECT
  metric_time__day AS metric_time__day
  , nested_fill_nulls_without_time_spine AS nested_fill_nulls_without_time_spine
FROM cm_8_cte cm_8_cte
