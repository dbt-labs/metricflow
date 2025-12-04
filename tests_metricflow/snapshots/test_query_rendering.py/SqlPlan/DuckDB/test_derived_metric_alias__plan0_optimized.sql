test_name: test_derived_metric_alias
test_filename: test_query_rendering.py
docstring:
  Tests a plan with an aliased metric.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Order By ['booking_fees']
-- Change Column Aliases
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , booking_value AS __booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , booking_value * 0.05 AS bookings_alias
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['__booking_value', 'metric_time__day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(booking_value) AS booking_value
  FROM (
    -- Join Standard Outputs
    -- Pass Only Elements: ['__booking_value', 'metric_time__day', 'listing__booking_fees']
    SELECT
      sma_28009_cte.metric_time__day AS metric_time__day
      , subq_31.listing__booking_fees AS listing__booking_fees
      , sma_28009_cte.__booking_value AS booking_value
    FROM sma_28009_cte
    LEFT OUTER JOIN (
      -- Compute Metrics via Expressions
      -- Pass Only Elements: ['listing', 'listing__booking_fees']
      SELECT
        listing
        , booking_value * 0.05 AS listing__booking_fees
      FROM (
        -- Read From CTE For node_id=sma_28009
        -- Pass Only Elements: ['__booking_value', 'listing']
        -- Pass Only Elements: ['__booking_value', 'listing']
        -- Aggregate Inputs for Simple Metrics
        -- Compute Metrics via Expressions
        SELECT
          listing
          , SUM(__booking_value) AS booking_value
        FROM sma_28009_cte
        GROUP BY
          listing
      ) subq_29
    ) subq_31
    ON
      sma_28009_cte.listing = subq_31.listing
  ) subq_33
  WHERE listing__booking_fees > 2
  GROUP BY
    metric_time__day
) subq_37
ORDER BY bookings_alias
