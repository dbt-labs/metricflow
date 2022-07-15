-- Compute Metrics via Expressions
SELECT
  metric_time
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Join Aggregated Measures with Standard Outputs
  -- Pass Only Elements:
  --   ['metric_time', 'average_booking_value', 'bookings', 'booking_value']
  SELECT
    subq_32.metric_time AS metric_time
    , subq_32.bookings AS bookings
    , subq_32.average_booking_value AS average_booking_value
    , subq_37.booking_value AS booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements:
    --   ['average_booking_value', 'bookings', 'metric_time']
    -- Aggregate Measures
    SELECT
      metric_time
      , SUM(bookings) AS bookings
      , AVG(average_booking_value) AS average_booking_value
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements:
      --   ['average_booking_value', 'bookings', 'listing__is_lux_latest', 'metric_time']
      SELECT
        subq_23.metric_time AS metric_time
        , listings_latest_src_10004.is_lux AS listing__is_lux_latest
        , subq_23.bookings AS bookings
        , subq_23.average_booking_value AS average_booking_value
      FROM (
        -- Read Elements From Data Source 'bookings_source'
        -- Pass Only Additive Measures
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements:
        --   ['average_booking_value', 'bookings', 'metric_time', 'listing']
        SELECT
          ds AS metric_time
          , listing_id AS listing
          , 1 AS bookings
          , booking_value AS average_booking_value
        FROM (
          -- User Defined SQL Query
          SELECT * FROM ***************************.fct_bookings
        ) bookings_source_src_10001
      ) subq_23
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_10004
      ON
        subq_23.listing = listings_latest_src_10004.listing_id
    ) subq_29
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time
  ) subq_32
  INNER JOIN (
    -- Read Elements From Data Source 'bookings_source'
    -- Pass Only Additive Measures
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['booking_value', 'metric_time']
    -- Aggregate Measures
    SELECT
      ds AS metric_time
      , SUM(booking_value) AS booking_value
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10001
    GROUP BY
      metric_time
  ) subq_37
  ON
    (
      (
        subq_32.metric_time = subq_37.metric_time
      ) OR (
        (subq_32.metric_time IS NULL) AND (subq_37.metric_time IS NULL)
      )
    )
) subq_39
