-- Compute Metrics via Expressions
SELECT
  metric_time
  , average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_41.metric_time, subq_53.metric_time, subq_58.metric_time) AS metric_time
    , subq_41.average_booking_value AS average_booking_value
    , subq_53.bookings AS bookings
    , subq_58.booking_value AS booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements:
    --   ['average_booking_value', 'metric_time']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time
      , AVG(average_booking_value) AS average_booking_value
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements:
      --   ['average_booking_value', 'listing__is_lux_latest', 'metric_time']
      SELECT
        bookings_source_src_10001.ds AS metric_time
        , listings_latest_src_10004.is_lux AS listing__is_lux_latest
        , bookings_source_src_10001.booking_value AS average_booking_value
      FROM ***************************.fct_bookings bookings_source_src_10001
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_10004
      ON
        bookings_source_src_10001.listing_id = listings_latest_src_10004.listing_id
    ) subq_37
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time
  ) subq_41
  INNER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements:
    --   ['bookings', 'metric_time']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time
      , SUM(bookings) AS bookings
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements:
      --   ['bookings', 'listing__is_lux_latest', 'metric_time']
      SELECT
        subq_44.metric_time AS metric_time
        , listings_latest_src_10004.is_lux AS listing__is_lux_latest
        , subq_44.bookings AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements:
        --   ['bookings', 'metric_time', 'listing']
        SELECT
          ds AS metric_time
          , listing_id AS listing
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_10001
      ) subq_44
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_10004
      ON
        subq_44.listing = listings_latest_src_10004.listing_id
    ) subq_49
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time
  ) subq_53
  ON
    (
      subq_41.metric_time = subq_53.metric_time
    ) OR (
      (subq_41.metric_time IS NULL) AND (subq_53.metric_time IS NULL)
    )
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['booking_value', 'metric_time']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      ds AS metric_time
      , SUM(booking_value) AS booking_value
    FROM ***************************.fct_bookings bookings_source_src_10001
    GROUP BY
      metric_time
  ) subq_58
  ON
    (
      subq_41.metric_time = subq_58.metric_time
    ) OR (
      (subq_41.metric_time IS NULL) AND (subq_58.metric_time IS NULL)
    )
) subq_59
