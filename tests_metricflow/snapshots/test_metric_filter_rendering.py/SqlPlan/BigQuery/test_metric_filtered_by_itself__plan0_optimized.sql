test_name: test_metric_filtered_by_itself
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query for a metric that filters by the same metric.
sql_engine: BigQuery
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__bookers']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , guest_id AS __bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  COUNT(DISTINCT __bookers) AS bookers
FROM (
  -- Join Standard Outputs
  SELECT
    subq_21.listing__bookers AS listing__bookers
    , subq_16.__bookers AS __bookers
  FROM (
    -- Read From CTE For node_id=sma_28009
    SELECT
      listing
      , __bookers
    FROM sma_28009_cte
  ) subq_16
  LEFT OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
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
  ) subq_21
  ON
    subq_16.listing = subq_21.listing
) subq_22
WHERE listing__bookers > 1.00
