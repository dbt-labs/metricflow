test_name: test_metric_filtered_by_itself
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query for a metric that filters by the same metric.
sql_engine: Postgres
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookers',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  COUNT(DISTINCT bookers) AS bookers
FROM (
  -- Join Standard Outputs
  SELECT
    nr_subq_16.listing__bookers AS listing__bookers
    , nr_subq_13.bookers AS bookers
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , guest_id AS bookers
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_13
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
  ) nr_subq_16
  ON
    nr_subq_13.listing = nr_subq_16.listing
) nr_subq_17
WHERE listing__bookers > 1.00
