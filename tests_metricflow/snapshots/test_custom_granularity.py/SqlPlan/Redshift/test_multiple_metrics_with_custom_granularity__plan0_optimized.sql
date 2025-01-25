test_name: test_multiple_metrics_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Redshift
---
-- Combine Aggregated Outputs
SELECT
  COALESCE(nr_subq_14.metric_time__martian_day, nr_subq_19.metric_time__martian_day) AS metric_time__martian_day
  , MAX(nr_subq_14.bookings) AS bookings
  , MAX(nr_subq_19.listings) AS listings
FROM (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    nr_subq_10.martian_day AS metric_time__martian_day
    , SUM(nr_subq_28002.bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    SELECT
      1 AS bookings
      , DATE_TRUNC('day', ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_28002
  LEFT OUTER JOIN
    ***************************.mf_time_spine nr_subq_10
  ON
    nr_subq_28002.ds__day = nr_subq_10.ds
  GROUP BY
    nr_subq_10.martian_day
) nr_subq_14
FULL OUTER JOIN (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['listings', 'metric_time__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    nr_subq_15.martian_day AS metric_time__martian_day
    , SUM(nr_subq_28007.listings) AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    SELECT
      1 AS listings
      , DATE_TRUNC('day', created_at) AS ds__day
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) nr_subq_28007
  LEFT OUTER JOIN
    ***************************.mf_time_spine nr_subq_15
  ON
    nr_subq_28007.ds__day = nr_subq_15.ds
  GROUP BY
    nr_subq_15.martian_day
) nr_subq_19
ON
  nr_subq_14.metric_time__martian_day = nr_subq_19.metric_time__martian_day
GROUP BY
  COALESCE(nr_subq_14.metric_time__martian_day, nr_subq_19.metric_time__martian_day)
