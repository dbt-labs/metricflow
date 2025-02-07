test_name: test_nested_filters
test_filename: test_derived_metric_rendering.py
docstring:
  Tests derived metric rendering for a nested derived metric with filters on the outer metric spec.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
WITH sma_28009_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  SELECT
    listing_id AS listing
    , is_instant AS booking__is_instant
    , 1 AS bookings
    , booking_value
    , booking_value AS average_booking_value
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
  instant_lux_booking_value_rate AS instant_lux_booking_value_rate
FROM (
  -- Compute Metrics via Expressions
  SELECT
    average_booking_value * bookings / NULLIF(booking_value, 0) AS instant_lux_booking_value_rate
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      MAX(subq_37.average_booking_value) AS average_booking_value
      , MAX(subq_45.bookings) AS bookings
      , MAX(subq_53.booking_value) AS booking_value
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['average_booking_value',]
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        AVG(average_booking_value) AS average_booking_value
      FROM (
        -- Join Standard Outputs
        SELECT
          sma_28014_cte.is_lux_latest AS listing__is_lux_latest
          , sma_28009_cte.booking__is_instant AS booking__is_instant
          , sma_28009_cte.average_booking_value AS average_booking_value
        FROM sma_28009_cte sma_28009_cte
        LEFT OUTER JOIN
          sma_28014_cte sma_28014_cte
        ON
          sma_28009_cte.listing = sma_28014_cte.listing
      ) subq_33
      WHERE (listing__is_lux_latest) AND (booking__is_instant)
    ) subq_37
    CROSS JOIN (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['bookings',]
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        SUM(bookings) AS bookings
      FROM (
        -- Join Standard Outputs
        SELECT
          sma_28014_cte.is_lux_latest AS listing__is_lux_latest
          , sma_28009_cte.booking__is_instant AS booking__is_instant
          , sma_28009_cte.bookings AS bookings
        FROM sma_28009_cte sma_28009_cte
        LEFT OUTER JOIN
          sma_28014_cte sma_28014_cte
        ON
          sma_28009_cte.listing = sma_28014_cte.listing
      ) subq_41
      WHERE (((listing__is_lux_latest) AND (booking__is_instant)) AND (listing__is_lux_latest)) AND (booking__is_instant)
    ) subq_45
    CROSS JOIN (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['booking_value',]
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        SUM(booking_value) AS booking_value
      FROM (
        -- Join Standard Outputs
        SELECT
          sma_28014_cte.is_lux_latest AS listing__is_lux_latest
          , sma_28009_cte.booking__is_instant AS booking__is_instant
          , sma_28009_cte.booking_value AS booking_value
        FROM sma_28009_cte sma_28009_cte
        LEFT OUTER JOIN
          sma_28014_cte sma_28014_cte
        ON
          sma_28009_cte.listing = sma_28014_cte.listing
      ) subq_49
      WHERE ((((listing__is_lux_latest) AND (booking__is_instant)) AND (listing__is_lux_latest)) AND (booking__is_instant)) AND (booking__is_instant)
    ) subq_53
  ) subq_54
) subq_55
