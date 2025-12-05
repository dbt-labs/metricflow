test_name: test_cumulative_metric_with_query_time_filters
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a query against a cumulative metric.

      TODO: support metric time filters
sql_engine: Trino
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__bookers', 'listing__country_latest', 'metric_time__day']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , listing__country_latest
  , COUNT(DISTINCT bookers) AS every_two_days_bookers
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__bookers', 'listing__country_latest', 'booking__is_instant', 'metric_time__day']
  SELECT
    subq_19.metric_time__day AS metric_time__day
    , subq_19.booking__is_instant AS booking__is_instant
    , listings_latest_src_28000.country AS listing__country_latest
    , subq_19.__bookers AS bookers
  FROM (
    -- Join Self Over Time Range
    SELECT
      subq_18.ds AS metric_time__day
      , bookings_source_src_28000.listing_id AS listing
      , bookings_source_src_28000.is_instant AS booking__is_instant
      , bookings_source_src_28000.guest_id AS __bookers
    FROM ***************************.mf_time_spine subq_18
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_18.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_28000.ds) > DATE_ADD('day', -2, subq_18.ds)
      )
  ) subq_19
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    subq_19.listing = listings_latest_src_28000.listing_id
) subq_24
WHERE booking__is_instant
GROUP BY
  metric_time__day
  , listing__country_latest
