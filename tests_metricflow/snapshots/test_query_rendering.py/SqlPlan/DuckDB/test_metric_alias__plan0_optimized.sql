test_name: test_metric_alias
test_filename: test_query_rendering.py
docstring:
  Tests a plan with an aliased metric.
sql_engine: DuckDB
---
-- Order By ['bookings']
-- Change Column Aliases
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('month', ds) AS metric_time__month
    , listing_id AS listing
    , 1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__month AS metric_time__month
  , bookings AS bookings
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__month']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__month
    , SUM(bookings) AS bookings
  FROM (
    -- Join Standard Outputs
    SELECT
      subq_20.listing__bookings AS listing__bookings
      , subq_15.metric_time__month AS metric_time__month
      , subq_15.bookings AS bookings
    FROM (
      -- Read From CTE For node_id=sma_28009
      SELECT
        metric_time__month
        , listing
        , bookings
      FROM sma_28009_cte sma_28009_cte
    ) subq_15
    LEFT OUTER JOIN (
      -- Read From CTE For node_id=sma_28009
      -- Pass Only Elements: ['bookings', 'listing']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      -- Pass Only Elements: ['listing', 'listing__bookings']
      SELECT
        listing
        , SUM(bookings) AS listing__bookings
      FROM sma_28009_cte sma_28009_cte
      GROUP BY
        listing
    ) subq_20
    ON
      subq_15.listing = subq_20.listing
  ) subq_21
  WHERE listing__bookings > 2
  GROUP BY
    metric_time__month
) subq_25
