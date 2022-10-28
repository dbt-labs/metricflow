-- Compute Metrics via Expressions
SELECT
  metric_time
  , non_referred + (instant * 1.0 / bookings) AS instant_plus_non_referred_bookings_pct
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_42.metric_time, subq_43.metric_time, subq_44.metric_time) AS metric_time
    , subq_42.non_referred AS non_referred
    , subq_43.instant AS instant
    , subq_44.bookings AS bookings
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time
      , (bookings - ref_bookings) * 1.0 / bookings AS non_referred
    FROM (
      -- Combine Metrics
      SELECT
        COALESCE(subq_31.metric_time, subq_32.metric_time) AS metric_time
        , subq_31.ref_bookings AS ref_bookings
        , subq_32.bookings AS bookings
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
        ) subq_25
        GROUP BY
          metric_time
      ) subq_31
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
        ) subq_29
        GROUP BY
          metric_time
      ) subq_32
      ON
        subq_31.metric_time = subq_32.metric_time
    ) subq_33
  ) subq_42
  INNER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time
      , SUM(instant_bookings) AS instant
    FROM (
      -- Read Elements From Data Source 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['instant_bookings', 'metric_time']
      SELECT
        ds AS metric_time
        , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_bookings
      ) bookings_source_src_10001
    ) subq_36
    GROUP BY
      metric_time
  ) subq_43
  ON
    subq_42.metric_time = subq_43.metric_time
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
    ) subq_40
    GROUP BY
      metric_time
  ) subq_44
  ON
    COALESCE(subq_42.metric_time, subq_43.metric_time) = subq_44.metric_time
) subq_45
