test_name: test_derived_metric_alias
test_filename: test_query_rendering.py
docstring:
  Tests a plan with an aliased metric.
sql_engine: Snowflake
---
-- Order By ['booking_fees']
-- Change Column Aliases
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , booking_fees AS booking_fees
FROM (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , booking_value * 0.05 AS booking_fees
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['booking_value', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(booking_value) AS booking_value
    FROM (
      -- Join Standard Outputs
      SELECT
        subq_23.listing__booking_fees AS listing__booking_fees
        , sma_28009_cte.metric_time__day AS metric_time__day
        , sma_28009_cte.booking_value AS booking_value
      FROM sma_28009_cte sma_28009_cte
      LEFT OUTER JOIN (
        -- Compute Metrics via Expressions
        -- Pass Only Elements: ['listing', 'listing__booking_fees']
        SELECT
          listing
          , booking_value * 0.05 AS listing__booking_fees
        FROM (
          -- Read From CTE For node_id=sma_28009
          -- Pass Only Elements: ['booking_value', 'listing']
          -- Aggregate Measures
          -- Compute Metrics via Expressions
          SELECT
            listing
            , SUM(booking_value) AS booking_value
          FROM sma_28009_cte sma_28009_cte
          GROUP BY
            listing
        ) subq_21
      ) subq_23
      ON
        sma_28009_cte.listing = subq_23.listing
    ) subq_24
    WHERE listing__booking_fees > 2
    GROUP BY
      metric_time__day
  ) subq_28
) subq_29
