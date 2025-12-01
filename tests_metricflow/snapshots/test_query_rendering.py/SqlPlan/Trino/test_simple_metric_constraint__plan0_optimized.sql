test_name: test_simple_metric_constraint
test_filename: test_query_rendering.py
sql_engine: Trino
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , listing_id AS listing
    , 1 AS __bookings
    , booking_value AS __average_booking_value
    , booking_value AS __booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
)

, sma_28014_cte AS (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , is_lux AS is_lux_latest
  FROM ***************************.dim_listings_latest listings_latest_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_33.metric_time__day, subq_41.metric_time__day, subq_45.metric_time__day) AS metric_time__day
    , MAX(subq_33.average_booking_value) AS average_booking_value
    , MAX(subq_41.bookings) AS bookings
    , MAX(subq_45.booking_value) AS booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__average_booking_value', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , AVG(average_booking_value) AS average_booking_value
    FROM (
      -- Join Standard Outputs
      SELECT
        sma_28014_cte.is_lux_latest AS listing__is_lux_latest
        , sma_28009_cte.metric_time__day AS metric_time__day
        , sma_28009_cte.__average_booking_value AS average_booking_value
      FROM sma_28009_cte
      LEFT OUTER JOIN
        sma_28014_cte
      ON
        sma_28009_cte.listing = sma_28014_cte.listing
    ) subq_29
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time__day
  ) subq_33
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__bookings', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Join Standard Outputs
      SELECT
        sma_28014_cte.is_lux_latest AS listing__is_lux_latest
        , sma_28009_cte.metric_time__day AS metric_time__day
        , sma_28009_cte.__bookings AS bookings
      FROM sma_28009_cte
      LEFT OUTER JOIN
        sma_28014_cte
      ON
        sma_28009_cte.listing = sma_28014_cte.listing
    ) subq_37
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time__day
  ) subq_41
  ON
    subq_33.metric_time__day = subq_41.metric_time__day
  FULL OUTER JOIN (
    -- Read From CTE For node_id=sma_28009
    -- Pass Only Elements: ['__booking_value', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(__booking_value) AS booking_value
    FROM sma_28009_cte
    GROUP BY
      metric_time__day
  ) subq_45
  ON
    COALESCE(subq_33.metric_time__day, subq_41.metric_time__day) = subq_45.metric_time__day
  GROUP BY
    COALESCE(subq_33.metric_time__day, subq_41.metric_time__day, subq_45.metric_time__day)
) subq_46
