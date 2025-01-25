test_name: test_id_enumeration
test_filename: test_rendered_query.py
sql_engine: BigQuery
---
-- Combine Aggregated Outputs
SELECT
  COALESCE(nr_subq_3.metric_time__day, nr_subq_7.metric_time__day) AS metric_time__day
  , MAX(nr_subq_3.bookings) AS bookings
  , MAX(nr_subq_7.listings) AS listings
FROM (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
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
    FROM ***************************.fct_bookings bookings_source_src_10000
  ) nr_subq_1
  GROUP BY
    metric_time__day
) nr_subq_3
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
    FROM ***************************.dim_listings_latest listings_latest_src_10000
  ) nr_subq_5
  GROUP BY
    metric_time__day
) nr_subq_7
ON
  nr_subq_3.metric_time__day = nr_subq_7.metric_time__day
GROUP BY
  metric_time__day
