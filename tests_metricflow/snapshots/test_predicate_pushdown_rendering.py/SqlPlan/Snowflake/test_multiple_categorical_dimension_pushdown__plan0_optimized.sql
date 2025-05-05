test_name: test_multiple_categorical_dimension_pushdown
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we expect predicate pushdown for more than one categorical dimension.
sql_engine: Snowflake
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listings', 'user__home_state_latest']
-- Aggregate Measures
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  user__home_state_latest
  , SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  SELECT
    users_latest_src_28000.home_state_latest AS user__home_state_latest
    , subq_10.listing__is_lux_latest AS listing__is_lux_latest
    , subq_10.listing__capacity_latest AS listing__capacity_latest
    , subq_10.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      user_id AS user
      , is_lux AS listing__is_lux_latest
      , capacity AS listing__capacity_latest
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_10
  LEFT OUTER JOIN
    ***************************.dim_users_latest users_latest_src_28000
  ON
    subq_10.user = users_latest_src_28000.user_id
) subq_13
WHERE listing__is_lux_latest OR listing__capacity_latest > 4
GROUP BY
  user__home_state_latest
