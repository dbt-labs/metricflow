-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_24.metric_time__day, subq_32.metric_time__day) AS metric_time__day
    , subq_24.month_start_bookings AS month_start_bookings
    , subq_32.bookings_1_month_ago AS bookings_1_month_ago
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_23.ds AS metric_time__day
      , subq_21.month_start_bookings AS month_start_bookings
    FROM ***************************.mf_time_spine subq_23
    INNER JOIN (
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , SUM(bookings) AS month_start_bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements:
        --   ['bookings', 'metric_time__day']
        SELECT
          ds AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_10001
      ) subq_19
      GROUP BY
        metric_time__day
    ) subq_21
    ON
      DATE_TRUNC('month', subq_23.ds) = subq_21.metric_time__day
  ) subq_24
  INNER JOIN (
    -- Join to Time Spine Dataset
    SELECT
      subq_31.ds AS metric_time__day
      , subq_29.bookings_1_month_ago AS bookings_1_month_ago
    FROM ***************************.mf_time_spine subq_31
    INNER JOIN (
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , SUM(bookings) AS bookings_1_month_ago
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements:
        --   ['bookings', 'metric_time__day']
        SELECT
          ds AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_10001
      ) subq_27
      GROUP BY
        metric_time__day
    ) subq_29
    ON
      subq_31.ds - INTERVAL 1 month = subq_29.metric_time__day
  ) subq_32
  ON
    (
      subq_24.metric_time__day = subq_32.metric_time__day
    ) OR (
      (
        subq_24.metric_time__day IS NULL
      ) AND (
        subq_32.metric_time__day IS NULL
      )
    )
) subq_33
