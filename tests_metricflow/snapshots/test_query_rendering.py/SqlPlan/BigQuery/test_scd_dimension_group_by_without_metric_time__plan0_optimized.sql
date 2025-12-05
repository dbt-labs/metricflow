test_name: test_scd_dimension_group_by_without_metric_time
test_filename: test_query_rendering.py
sql_engine: BigQuery
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__family_bookings', 'listing__capacity']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  listing__capacity
  , SUM(family_bookings) AS family_bookings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__family_bookings', 'listing__capacity']
  SELECT
    listings_src_26000.capacity AS listing__capacity
    , subq_11.__family_bookings AS family_bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , listing_id AS listing
      , 1 AS __family_bookings
    FROM ***************************.fct_bookings bookings_source_src_26000
  ) subq_11
  LEFT OUTER JOIN
    ***************************.dim_listings listings_src_26000
  ON
    (
      subq_11.listing = listings_src_26000.listing_id
    ) AND (
      (
        subq_11.metric_time__day >= listings_src_26000.active_from
      ) AND (
        (
          subq_11.metric_time__day < listings_src_26000.active_to
        ) OR (
          listings_src_26000.active_to IS NULL
        )
      )
    )
) subq_15
WHERE listing__capacity > 2
GROUP BY
  listing__capacity
