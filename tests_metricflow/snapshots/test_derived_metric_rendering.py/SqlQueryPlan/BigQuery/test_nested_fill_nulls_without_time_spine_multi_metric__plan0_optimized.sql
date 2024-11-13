test_name: test_nested_fill_nulls_without_time_spine_multi_metric
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Combine Aggregated Outputs
WITH cm_8_cte AS (
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
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_14
    GROUP BY
      metric_time__day
  ) subq_15
)

, cm_9_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , 2 * bookings_fill_nulls_with_0_without_time_spine AS twice_bookings_fill_nulls_with_0_without_time_spine
  FROM (
    -- Read From CTE For node_id=cm_8
    SELECT
      metric_time__day
      , bookings_fill_nulls_with_0_without_time_spine
    FROM cm_8_cte cm_8_cte
  ) subq_16
)

, cm_10_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , 3 * twice_bookings_fill_nulls_with_0_without_time_spine AS nested_fill_nulls_without_time_spine
  FROM (
    -- Read From CTE For node_id=cm_9
    SELECT
      metric_time__day
      , twice_bookings_fill_nulls_with_0_without_time_spine
    FROM cm_9_cte cm_9_cte
  ) subq_17
)

, cm_11_cte AS (
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
      DATETIME_TRUNC(created_at, day) AS metric_time__day
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_21
  GROUP BY
    metric_time__day
)

SELECT
  COALESCE(cm_10_cte.metric_time__day, cm_11_cte.metric_time__day) AS metric_time__day
  , MAX(cm_10_cte.nested_fill_nulls_without_time_spine) AS nested_fill_nulls_without_time_spine
  , MAX(cm_11_cte.listings) AS listings
FROM cm_10_cte cm_10_cte
FULL OUTER JOIN
  cm_11_cte cm_11_cte
ON
  cm_10_cte.metric_time__day = cm_11_cte.metric_time__day
GROUP BY
  metric_time__day
