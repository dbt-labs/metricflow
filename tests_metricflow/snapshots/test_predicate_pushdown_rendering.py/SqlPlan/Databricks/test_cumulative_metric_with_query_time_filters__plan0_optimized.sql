test_name: test_cumulative_metric_with_query_time_filters
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a query against a cumulative metric.

      TODO: support metric time filters
sql_engine: Databricks
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookers', 'listing__country_latest', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , listing__country_latest
  , COUNT(DISTINCT bookers) AS every_two_days_bookers
FROM (
  -- Join Standard Outputs
  SELECT
    listings_latest_src_28000.country AS listing__country_latest
    , nr_subq_13.metric_time__day AS metric_time__day
    , nr_subq_13.booking__is_instant AS booking__is_instant
    , nr_subq_13.bookers AS bookers
  FROM (
    -- Join Self Over Time Range
    SELECT
      nr_subq_12.ds AS metric_time__day
      , bookings_source_src_28000.listing_id AS listing
      , bookings_source_src_28000.is_instant AS booking__is_instant
      , bookings_source_src_28000.guest_id AS bookers
    FROM ***************************.mf_time_spine nr_subq_12
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        DATE_TRUNC('day', bookings_source_src_28000.ds) <= nr_subq_12.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_28000.ds) > DATEADD(day, -2, nr_subq_12.ds)
      )
  ) nr_subq_13
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    nr_subq_13.listing = listings_latest_src_28000.listing_id
) nr_subq_16
WHERE booking__is_instant
GROUP BY
  metric_time__day
  , listing__country_latest
