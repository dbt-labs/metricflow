test_name: test_multi_metric_fill_null
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Combine Aggregated Outputs
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
    ) subq_13
    GROUP BY
      metric_time__day
  ) subq_14
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
  ) subq_15
)

, cm_8_cte AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(listings) AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', created_at) AS metric_time__day
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_19
  GROUP BY
    metric_time__day
)

SELECT
  COALESCE(cm_7_cte.metric_time__day, cm_8_cte.metric_time__day) AS metric_time__day
  , MAX(cm_7_cte.twice_bookings_fill_nulls_with_0_without_time_spine) AS twice_bookings_fill_nulls_with_0_without_time_spine
  , MAX(cm_8_cte.listings) AS listings
FROM cm_7_cte cm_7_cte
FULL OUTER JOIN
  cm_8_cte cm_8_cte
ON
  cm_7_cte.metric_time__day = cm_8_cte.metric_time__day
GROUP BY
  COALESCE(cm_7_cte.metric_time__day, cm_8_cte.metric_time__day)
