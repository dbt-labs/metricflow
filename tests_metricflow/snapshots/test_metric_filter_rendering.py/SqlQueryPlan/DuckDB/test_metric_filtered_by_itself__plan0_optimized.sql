-- Constrain Output with WHERE
-- Pass Only Elements: ['bookers',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  COUNT(DISTINCT bookers) AS bookers
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['bookers', 'listing__bookers']
  SELECT
    subq_22.listing__bookers AS listing__bookers
    , subq_16.bookers AS bookers
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookers', 'listing']
    SELECT
      listing_id AS listing
      , guest_id AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_16
  LEFT OUTER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookers', 'listing']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookers']
    SELECT
      listing_id AS listing
      , COUNT(DISTINCT guest_id) AS listing__bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
    GROUP BY
      listing_id
  ) subq_22
  ON
    subq_16.listing = subq_22.listing
) subq_24
WHERE listing__bookers > 1.00
