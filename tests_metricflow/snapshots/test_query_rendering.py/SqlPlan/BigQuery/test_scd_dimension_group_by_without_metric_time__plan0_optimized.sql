test_name: test_scd_dimension_group_by_without_metric_time
test_filename: test_query_rendering.py
sql_engine: BigQuery
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'listing__capacity']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  listing__capacity
  , SUM(bookings) AS family_bookings
FROM (
  -- Join Standard Outputs
  SELECT
    listings_src_26000.capacity AS listing__capacity
    , subq_9.bookings AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
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
GROUP BY
  listing__capacity