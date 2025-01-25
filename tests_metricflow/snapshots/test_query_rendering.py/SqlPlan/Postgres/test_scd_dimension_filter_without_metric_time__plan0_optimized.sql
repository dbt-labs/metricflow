test_name: test_scd_dimension_filter_without_metric_time
test_filename: test_query_rendering.py
sql_engine: Postgres
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(bookings) AS family_bookings
FROM (
  -- Join Standard Outputs
  SELECT
    listings_src_26000.capacity AS listing__capacity
    , subq_9.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_26000
  ) subq_9
  LEFT OUTER JOIN
    ***************************.dim_listings listings_src_26000
  ON
    (
      subq_9.listing = listings_src_26000.listing_id
    ) AND (
      (
        subq_9.metric_time__day >= listings_src_26000.active_from
      ) AND (
        (
          subq_9.metric_time__day < listings_src_26000.active_to
        ) OR (
          listings_src_26000.active_to IS NULL
        )
      )
    )
) subq_12
WHERE listing__capacity > 2
