test_name: test_distinct_values_query_with_metric_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a distinct values query with a metric in the query-level where filter.
sql_engine: Redshift
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listing',]
SELECT
  listing
FROM (
  -- Join Standard Outputs
  SELECT
    nr_subq_14.listing__bookings AS listing__bookings
    , lux_listing_mapping_src_28000.listing_id AS listing
  FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_28000
  FULL OUTER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookings']
    SELECT
      listing
      , SUM(bookings) AS listing__bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'listing']
      SELECT
        listing_id AS listing
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_5
    GROUP BY
      listing
  ) nr_subq_14
  ON
    lux_listing_mapping_src_28000.listing_id = nr_subq_14.listing
) nr_subq_15
WHERE listing__bookings > 2
GROUP BY
  listing
