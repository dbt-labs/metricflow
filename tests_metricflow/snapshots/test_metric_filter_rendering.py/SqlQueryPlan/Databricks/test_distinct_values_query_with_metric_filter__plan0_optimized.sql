-- Constrain Output with WHERE
-- Pass Only Elements: ['listing',]
SELECT
  listing
FROM (
  -- Join Standard Outputs
  SELECT
    lux_listing_mapping_src_28000.listing_id AS listing
    , subq_19.listing__bookings AS listing__bookings
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
    ) subq_16
    GROUP BY
      listing
  ) subq_19
  ON
    lux_listing_mapping_src_28000.listing_id = subq_19.listing
) subq_20
WHERE listing__bookings > 2
GROUP BY
  listing
