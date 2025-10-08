test_name: test_nested_fill_nulls_without_time_spine
test_filename: test_derived_metric_rendering.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , 3 * twice_bookings_fill_nulls_with_0_without_time_spine AS nested_fill_nulls_without_time_spine
FROM (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , 2 * bookings_fill_nulls_with_0_without_time_spine AS twice_bookings_fill_nulls_with_0_without_time_spine
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , COALESCE(bookings_fill_nulls_with_0_without_time_spine, 0) AS bookings_fill_nulls_with_0_without_time_spine
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        metric_time__day
        , SUM(bookings_fill_nulls_with_0_without_time_spine) AS bookings_fill_nulls_with_0_without_time_spine
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['bookings_fill_nulls_with_0_without_time_spine', 'metric_time__day']
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS bookings_fill_nulls_with_0_without_time_spine
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_9
      GROUP BY
        metric_time__day
    ) subq_10
  ) subq_11
) subq_12
