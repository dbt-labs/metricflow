test_name: test_nested_filters
test_filename: test_derived_metric_rendering.py
docstring:
  Tests derived metric rendering for a nested derived metric with filters on the outer metric spec.
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    listing_id AS listing
    , is_instant AS booking__is_instant
    , 1 AS __bookings
    , booking_value AS __average_booking_value
    , booking_value AS __booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  instant_lux_booking_value_rate AS instant_lux_booking_value_rate
FROM (
  SELECT
    average_booking_value * bookings / NULLIF(booking_value, 0) AS instant_lux_booking_value_rate
  FROM (
    SELECT
      MAX(subq_39.average_booking_value) AS average_booking_value
      , MAX(subq_39.bookings) AS bookings
      , MAX(subq_45.booking_value) AS booking_value
    FROM (
      SELECT
        AVG(__average_booking_value) AS average_booking_value
        , SUM(__bookings) AS bookings
      FROM (
        SELECT
          bookings AS __bookings
          , average_booking_value AS __average_booking_value
        FROM (
          SELECT
            sma_28009_cte.booking__is_instant AS booking__is_instant
            , listings_latest_src_28000.is_lux AS listing__is_lux_latest
            , sma_28009_cte.__bookings AS bookings
            , sma_28009_cte.__average_booking_value AS average_booking_value
          FROM sma_28009_cte
          LEFT OUTER JOIN
            ***************************.dim_listings_latest listings_latest_src_28000
          ON
            sma_28009_cte.listing = listings_latest_src_28000.listing_id
        ) subq_35
        WHERE (listing__is_lux_latest) AND (booking__is_instant)
      ) subq_37
    ) subq_39
    CROSS JOIN (
      SELECT
        SUM(__booking_value) AS booking_value
      FROM (
        SELECT
          booking_value AS __booking_value
        FROM (
          SELECT
            booking__is_instant
            , __booking_value AS booking_value
          FROM sma_28009_cte
        ) subq_41
        WHERE booking__is_instant
      ) subq_43
    ) subq_45
  ) subq_46
) subq_47
