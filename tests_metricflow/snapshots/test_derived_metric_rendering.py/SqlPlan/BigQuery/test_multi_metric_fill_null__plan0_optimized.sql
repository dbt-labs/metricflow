test_name: test_multi_metric_fill_null
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Combine Aggregated Outputs
SELECT
  COALESCE(nr_subq_13.metric_time__day, nr_subq_17.metric_time__day) AS metric_time__day
  , MAX(nr_subq_13.twice_bookings_fill_nulls_with_0_without_time_spine) AS twice_bookings_fill_nulls_with_0_without_time_spine
  , MAX(nr_subq_17.listings) AS listings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , 2 * bookings_fill_nulls_with_0_without_time_spine AS twice_bookings_fill_nulls_with_0_without_time_spine
  FROM (
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
      ) nr_subq_10
      GROUP BY
        metric_time__day
    ) nr_subq_11
  ) nr_subq_12
) nr_subq_13
FULL OUTER JOIN (
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
  ) nr_subq_15
  GROUP BY
    metric_time__day
) nr_subq_17
ON
  nr_subq_13.metric_time__day = nr_subq_17.metric_time__day
GROUP BY
  metric_time__day
