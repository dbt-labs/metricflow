test_name: test_query_with_multiple_metrics_in_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with 2 simple metrics in the query-level where filter.
sql_engine: Postgres
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__listings']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , 1 AS __bookings
    , guest_id AS __bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__listings', 'listing__bookings', 'listing__bookers']
  SELECT
    subq_37.listing__bookings AS listing__bookings
    , subq_43.listing__bookers AS listing__bookers
    , subq_30.__listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      listing_id AS listing
      , 1 AS __listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_30
  LEFT OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['__bookings', 'listing']
    -- Pass Only Elements: ['__bookings', 'listing']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookings']
    SELECT
      listing
      , SUM(__bookings) AS listing__bookings
    FROM sma_28009_cte
    GROUP BY
      listing
  ) subq_37
  ON
    subq_30.listing = subq_37.listing
  LEFT OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['__bookers', 'listing']
    -- Pass Only Elements: ['__bookers', 'listing']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookers']
    SELECT
      listing
      , COUNT(DISTINCT __bookers) AS listing__bookers
    FROM sma_28009_cte
    GROUP BY
      listing
  ) subq_43
  ON
    subq_30.listing = subq_43.listing
) subq_45
WHERE listing__bookings > 2 AND listing__bookers > 1
