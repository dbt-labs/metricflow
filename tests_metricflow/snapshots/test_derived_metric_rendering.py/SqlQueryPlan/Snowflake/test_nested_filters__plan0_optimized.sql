test_name: test_nested_filters
test_filename: test_derived_metric_rendering.py
docstring:
  Tests derived metric rendering for a nested derived metric with filters on the outer metric spec.
sql_engine: Snowflake
---
-- Read From CTE For node_id=cm_15
WITH cm_13_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['average_booking_value', 'bookings']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    AVG(average_booking_value) AS average_booking_value
    , SUM(bookings) AS bookings
  FROM (
    -- Join Standard Outputs
    SELECT
      listings_latest_src_28000.is_lux AS listing__is_lux_latest
      , subq_29.booking__is_instant AS booking__is_instant
      , subq_29.bookings AS bookings
      , subq_29.average_booking_value AS average_booking_value
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        listing_id AS listing
        , is_instant AS booking__is_instant
        , 1 AS bookings
        , booking_value AS average_booking_value
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_29
    LEFT OUTER JOIN
      ***************************.dim_listings_latest listings_latest_src_28000
    ON
      subq_29.listing = listings_latest_src_28000.listing_id
  ) subq_33
  WHERE (listing__is_lux_latest) AND (booking__is_instant)
)

, cm_12_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['booking_value',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(booking_value) AS booking_value
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      is_instant AS booking__is_instant
      , booking_value
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_39
  WHERE booking__is_instant
)

, cm_14_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    average_booking_value * bookings / NULLIF(booking_value, 0) AS instant_lux_booking_value_rate
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      MAX(cm_13_cte.average_booking_value) AS average_booking_value
      , MAX(cm_13_cte.bookings) AS bookings
      , MAX(cm_12_cte.booking_value) AS booking_value
    FROM cm_13_cte cm_13_cte
    CROSS JOIN
      cm_12_cte cm_12_cte
  ) subq_44
)

, cm_15_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    instant_lux_booking_value_rate AS instant_lux_booking_value_rate
  FROM (
    -- Read From CTE For node_id=cm_14
    SELECT
      instant_lux_booking_value_rate
    FROM cm_14_cte cm_14_cte
  ) subq_45
)

SELECT
  instant_lux_booking_value_rate AS instant_lux_booking_value_rate
FROM cm_15_cte cm_15_cte
