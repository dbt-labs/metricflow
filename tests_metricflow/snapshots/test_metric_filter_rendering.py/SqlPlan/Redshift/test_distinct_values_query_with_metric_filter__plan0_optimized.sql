test_name: test_distinct_values_query_with_metric_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a distinct values query with a metric in the query-level where filter.
sql_engine: Redshift
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listing']
-- Write to DataTable
SELECT
  listing
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['listing', 'listing__bookings']
  SELECT
    lux_listing_mapping_src_28000.listing_id AS listing
    , subq_23.listing__bookings AS listing__bookings
  FROM ***************************.dim_lux_listing_id_mapping lux_listing_mapping_src_28000
  FULL OUTER JOIN (
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookings']
    SELECT
      listing
      , SUM(__bookings) AS listing__bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['__bookings', 'listing']
      -- Pass Only Elements: ['__bookings', 'listing']
      SELECT
        listing_id AS listing
        , 1 AS __bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_20
    GROUP BY
      listing
  ) subq_23
  ON
    lux_listing_mapping_src_28000.listing_id = subq_23.listing
) subq_25
WHERE listing__bookings > 2
GROUP BY
  listing
