test_name: test_aliases_with_metrics
test_filename: test_query_rendering.py
docstring:
  Tests a metric query with various aliases.
sql_engine: DuckDB
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'listing__capacity_latest', 'metric_time__day', 'listing']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Order By ['bookings', 'metric_time__day', 'listing__capacity_latest', 'listing']
-- Change Column Aliases
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , booking_value
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS booking_day
  , listing AS listing_id
  , listing__capacity_latest AS listing_capacity
  , SUM(bookings) AS bookings_alias
FROM (
  -- Join Standard Outputs
  SELECT
    subq_29.listing__booking_fees AS listing__booking_fees
    , listings_latest_src_28000.capacity AS listing__capacity_latest
    , sma_28009_cte.metric_time__day AS metric_time__day
    , sma_28009_cte.listing AS listing
    , sma_28009_cte.bookings AS bookings
  FROM sma_28009_cte
  LEFT OUTER JOIN (
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__booking_fees']
    SELECT
      listing
      , booking_value * 0.05 AS listing__booking_fees
    FROM (
      -- Read From CTE For node_id=sma_28009
      -- Pass Only Elements: ['booking_value', 'listing']
      -- Aggregate Inputs for Simple Metrics
      -- Compute Metrics via Expressions
      SELECT
        listing
        , SUM(booking_value) AS booking_value
      FROM sma_28009_cte
      GROUP BY
        listing
    ) subq_27
  ) subq_29
  ON
    sma_28009_cte.listing = subq_29.listing
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_28000
  ON
    sma_28009_cte.listing = listings_latest_src_28000.listing_id
) subq_33
WHERE listing__booking_fees > 2
GROUP BY
  metric_time__day
  , listing
  , listing__capacity_latest
ORDER BY bookings_alias, booking_day, listing_capacity, listing_id
