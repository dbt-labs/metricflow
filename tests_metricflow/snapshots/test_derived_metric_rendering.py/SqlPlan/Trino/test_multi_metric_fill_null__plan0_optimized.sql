test_name: test_multi_metric_fill_null
test_filename: test_derived_metric_rendering.py
sql_engine: Trino
---
-- Combine Aggregated Outputs
-- Write to DataTable
SELECT
  COALESCE(subq_17.metric_time__day, subq_22.metric_time__day) AS metric_time__day
  , MAX(subq_17.twice_bookings_fill_nulls_with_0_without_time_spine) AS twice_bookings_fill_nulls_with_0_without_time_spine
  , MAX(subq_22.listings) AS listings
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
      ) subq_14
      GROUP BY
        metric_time__day
    ) subq_15
  ) subq_16
) subq_17
FULL OUTER JOIN (
  -- Aggregate Inputs for Simple Metrics
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
  ) subq_20
  GROUP BY
    metric_time__day
) subq_22
ON
  subq_17.metric_time__day = subq_22.metric_time__day
GROUP BY
  COALESCE(subq_17.metric_time__day, subq_22.metric_time__day)
