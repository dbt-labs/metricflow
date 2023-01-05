-- Compute Metrics via Expressions
SELECT
  metric_time
  , (bookings - ref_bookings) * 1.0 / bookings AS non_referred_bookings_pct
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_15.metric_time, subq_20.metric_time) AS metric_time
    , subq_15.ref_bookings AS ref_bookings
    , subq_20.bookings AS bookings
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time
      , SUM(referred_bookings) AS ref_bookings
    FROM (
      -- Read Elements From Data Source 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['referred_bookings', 'metric_time']
      SELECT
        ds AS metric_time
        , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_bookings
      ) bookings_source_src_10001
    ) subq_13
    GROUP BY
      metric_time
  ) subq_15
  INNER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Data Source 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['bookings', 'metric_time']
      SELECT
        ds AS metric_time
        , 1 AS bookings
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_bookings
      ) bookings_source_src_10001
    ) subq_18
    GROUP BY
      metric_time
  ) subq_20
  ON
    (
      subq_15.metric_time = subq_20.metric_time
    ) OR (
      (subq_15.metric_time IS NULL) AND (subq_20.metric_time IS NULL)
    )
) subq_21
