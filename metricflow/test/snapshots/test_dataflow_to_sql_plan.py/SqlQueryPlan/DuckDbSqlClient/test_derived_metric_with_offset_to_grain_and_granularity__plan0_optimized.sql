-- Compute Metrics via Expressions
SELECT
  metric_time__week
  , bookings - bookings_at_start_of_month AS bookings_growth_since_start_of_month
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_18.metric_time__week, subq_26.metric_time__week) AS metric_time__week
    , subq_18.bookings AS bookings
    , subq_26.bookings_at_start_of_month AS bookings_at_start_of_month
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__week
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Data Source 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['bookings', 'metric_time__week']
      SELECT
        DATE_TRUNC('week', ds) AS metric_time__week
        , 1 AS bookings
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_bookings
      ) bookings_source_src_10001
    ) subq_16
    GROUP BY
      metric_time__week
  ) subq_18
  INNER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__week']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_21.metric_time__week AS metric_time__week
      , SUM(subq_20.bookings) AS bookings_at_start_of_month
    FROM (
      -- Date Spine
      SELECT
        DATE_TRUNC('week', ds) AS metric_time__week
      FROM ***************************.mf_time_spine subq_22
      GROUP BY
        DATE_TRUNC('week', ds)
    ) subq_21
    INNER JOIN (
      -- Read Elements From Data Source 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('week', ds) AS metric_time__week
        , 1 AS bookings
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_bookings
      ) bookings_source_src_10001
    ) subq_20
    ON
      DATE_TRUNC('month', subq_21.metric_time__week) = subq_20.metric_time__week
    GROUP BY
      subq_21.metric_time__week
  ) subq_26
  ON
    (
      subq_18.metric_time__week = subq_26.metric_time__week
    ) OR (
      (
        subq_18.metric_time__week IS NULL
      ) AND (
        subq_26.metric_time__week IS NULL
      )
    )
) subq_27
