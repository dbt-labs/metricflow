-- Compute Metrics via Expressions
SELECT
  metric_time
  , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_24.metric_time, subq_32.metric_time) AS metric_time
    , subq_24.month_start_bookings AS month_start_bookings
    , subq_32.bookings_1_month_ago AS bookings_1_month_ago
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_23.ds AS metric_time
      , subq_21.month_start_bookings AS month_start_bookings
    FROM ***************************.mf_time_spine subq_23
    INNER JOIN (
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        metric_time
        , SUM(bookings) AS month_start_bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements:
        --   ['bookings', 'metric_time']
        SELECT
          ds AS metric_time
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_10001
      ) subq_19
      GROUP BY
        metric_time
    ) subq_21
    ON
      DATE_TRUNC('month', subq_23.ds) = subq_21.metric_time
  ) subq_24
  INNER JOIN (
    -- Join to Time Spine Dataset
    SELECT
      subq_31.ds AS metric_time
      , subq_29.bookings_1_month_ago AS bookings_1_month_ago
    FROM ***************************.mf_time_spine subq_31
    INNER JOIN (
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        metric_time
        , SUM(bookings) AS bookings_1_month_ago
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements:
        --   ['bookings', 'metric_time']
        SELECT
          ds AS metric_time
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_10001
      ) subq_27
      GROUP BY
        metric_time
    ) subq_29
    ON
      subq_31.ds - MAKE_INTERVAL(months => 1) = subq_29.metric_time
  ) subq_32
  ON
    (
      subq_24.metric_time = subq_32.metric_time
    ) OR (
      (subq_24.metric_time IS NULL) AND (subq_32.metric_time IS NULL)
    )
) subq_33
