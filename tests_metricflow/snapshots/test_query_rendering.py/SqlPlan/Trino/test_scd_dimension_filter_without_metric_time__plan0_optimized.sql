test_name: test_scd_dimension_filter_without_metric_time
test_filename: test_query_rendering.py
sql_engine: Trino
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['family_bookings']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  SUM(family_bookings) AS family_bookings
FROM (
  -- Join Standard Outputs
  SELECT
    listings_src_26000.capacity AS listing__capacity
    , subq_10.family_bookings AS family_bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS family_bookings
    FROM ***************************.fct_bookings bookings_source_src_26000
  ) subq_10
  LEFT OUTER JOIN
    ***************************.dim_listings listings_src_26000
  ON
    (
      subq_10.listing = listings_src_26000.listing_id
    ) AND (
      (
        subq_10.metric_time__day >= listings_src_26000.active_from
      ) AND (
        (
          subq_10.metric_time__day < listings_src_26000.active_to
        ) OR (
          listings_src_26000.active_to IS NULL
        )
      )
    )
) subq_13
WHERE listing__capacity > 2
