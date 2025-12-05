test_name: test_metric_filtered_by_itself
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query for a metric that filters by the same metric.
sql_engine: Databricks
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
  COUNT(DISTINCT bookers) AS bookers
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__bookers', 'listing__bookers']
  SELECT
    subq_25.listing__bookers AS listing__bookers
    , subq_19.__bookers AS bookers
  FROM (
    -- Read From CTE For node_id=sma_28009
    SELECT
      listing
      , __bookers
    FROM sma_28009_cte
  ) subq_19
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
  ) subq_25
  ON
    subq_19.listing = subq_25.listing
) subq_27
WHERE listing__bookers > 1.00
