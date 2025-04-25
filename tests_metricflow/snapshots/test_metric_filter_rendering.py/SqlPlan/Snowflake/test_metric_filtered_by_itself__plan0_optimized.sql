test_name: test_metric_filtered_by_itself
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query for a metric that filters by the same metric.
sql_engine: Snowflake
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['bookers']
-- Aggregate Measures
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , guest_id AS bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  COUNT(DISTINCT bookers) AS bookers
FROM (
  -- Join Standard Outputs
  SELECT
    subq_20.listing__bookers AS listing__bookers
    , subq_15.bookers AS bookers
  FROM (
    -- Read From CTE For node_id=sma_28009
    SELECT
      listing
      , bookers
    FROM sma_28009_cte sma_28009_cte
  ) subq_15
  LEFT OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['bookers', 'listing']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    -- Pass Only Elements: ['listing', 'listing__bookers']
    SELECT
      listing
      , COUNT(DISTINCT bookers) AS listing__bookers
    FROM sma_28009_cte sma_28009_cte
    GROUP BY
      listing
  ) subq_20
  ON
    subq_15.listing = subq_20.listing
) subq_21
WHERE listing__bookers > 1.00
