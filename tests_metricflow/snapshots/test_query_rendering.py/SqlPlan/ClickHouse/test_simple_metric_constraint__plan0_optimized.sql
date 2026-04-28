test_name: test_simple_metric_constraint
test_filename: test_query_rendering.py
sql_engine: ClickHouse
---
WITH sma_28009_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , listing_id AS listing
    , 1 AS __bookings
    , booking_value AS __average_booking_value
    , booking_value AS __booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  SELECT
    COALESCE(subq_37.metric_time__day, subq_42.metric_time__day) AS metric_time__day
    , MAX(subq_37.average_booking_value) AS average_booking_value
    , MAX(subq_37.bookings) AS bookings
    , MAX(subq_42.booking_value) AS booking_value
  FROM (
    SELECT
      metric_time__day
      , AVG(__average_booking_value) AS average_booking_value
      , SUM(__bookings) AS bookings
    FROM (
      SELECT
        metric_time__day
        , bookings AS __bookings
        , average_booking_value AS __average_booking_value
      FROM (
        SELECT
          sma_28009_cte.metric_time__day AS metric_time__day
          , listings_latest_src_28000.is_lux AS listing__is_lux_latest
          , sma_28009_cte.__bookings AS bookings
          , sma_28009_cte.__average_booking_value AS average_booking_value
        FROM sma_28009_cte
        LEFT OUTER JOIN
          ***************************.dim_listings_latest listings_latest_src_28000
        ON
          sma_28009_cte.listing = listings_latest_src_28000.listing_id
      ) subq_33
      WHERE listing__is_lux_latest
    ) subq_35
    GROUP BY
      metric_time__day
  ) subq_37
  FULL OUTER JOIN (
    SELECT
      metric_time__day
      , SUM(__booking_value) AS booking_value
    FROM sma_28009_cte
    GROUP BY
      metric_time__day
  ) subq_42
  ON
    subq_37.metric_time__day = subq_42.metric_time__day
  GROUP BY
    COALESCE(subq_37.metric_time__day, subq_42.metric_time__day)
) subq_43
