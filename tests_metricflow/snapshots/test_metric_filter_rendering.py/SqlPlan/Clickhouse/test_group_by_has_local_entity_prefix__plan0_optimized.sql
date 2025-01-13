test_name: test_group_by_has_local_entity_prefix
test_filename: test_metric_filter_rendering.py
sql_engine: Clickhouse
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
WITH sma_28014_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , user_id AS user
    , 1 AS listings
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
)

SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  SELECT
    subq_26.listing__user__average_booking_value AS user__listing__user__average_booking_value
    , sma_28014_cte.listings AS listings
  FROM sma_28014_cte sma_28014_cte
  LEFT OUTER JOIN
  (
    -- Join Standard Outputs
    -- Pass Only Elements: ['average_booking_value', 'listing__user']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing__user', 'listing__user__average_booking_value']
    SELECT
      sma_28014_cte.user AS listing__user
      , AVG(bookings_source_src_28000.booking_value) AS listing__user__average_booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
    LEFT OUTER JOIN
      sma_28014_cte sma_28014_cte
    ON
      bookings_source_src_28000.listing_id = sma_28014_cte.listing
    GROUP BY
      sma_28014_cte.user
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_26
  ON
    sma_28014_cte.user = subq_26.listing__user
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_27
WHERE user__listing__user__average_booking_value > 1
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
