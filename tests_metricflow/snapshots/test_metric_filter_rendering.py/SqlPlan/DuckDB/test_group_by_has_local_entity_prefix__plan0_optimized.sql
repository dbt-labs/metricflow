test_name: test_group_by_has_local_entity_prefix
test_filename: test_metric_filter_rendering.py
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__listings']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28014_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , user_id AS user
    , 1 AS __listings
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__listings', 'user__listing__user__average_booking_value']
  SELECT
    subq_36.listing__user__average_booking_value AS user__listing__user__average_booking_value
    , sma_28014_cte.__listings AS listings
  FROM sma_28014_cte
  LEFT OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements: ['__average_booking_value', 'listing__user']
    -- Pass Only Elements: ['__average_booking_value', 'listing__user']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing__user', 'listing__user__average_booking_value']
    SELECT
      sma_28014_cte.user AS listing__user
      , AVG(bookings_source_src_28000.booking_value) AS listing__user__average_booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
    LEFT OUTER JOIN
      sma_28014_cte
    ON
      bookings_source_src_28000.listing_id = sma_28014_cte.listing
    GROUP BY
      sma_28014_cte.user
  ) subq_36
  ON
    sma_28014_cte.user = subq_36.listing__user
) subq_38
WHERE user__listing__user__average_booking_value > 1
