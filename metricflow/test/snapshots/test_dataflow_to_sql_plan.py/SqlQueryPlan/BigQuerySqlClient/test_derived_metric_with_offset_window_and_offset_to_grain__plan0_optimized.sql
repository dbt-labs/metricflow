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
    -- Pass Only Elements:
    --   ['bookings', 'metric_time']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC(subq_19.metric_time, day) AS metric_time
      , SUM(subq_18.bookings) AS month_start_bookings
    FROM (
      -- Date Spine
      SELECT
        ds AS metric_time
      FROM ***************************.mf_time_spine subq_20
      GROUP BY
        metric_time
    ) subq_19
    INNER JOIN (
      -- Read Elements From Data Source 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        ds AS metric_time
        , 1 AS bookings
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_bookings
      ) bookings_source_src_10001
    ) subq_18
    ON
      DATE_TRUNC(subq_19.metric_time, month) = subq_18.metric_time
    GROUP BY
      metric_time
  ) subq_24
  INNER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements:
    --   ['bookings', 'metric_time']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC(subq_27.metric_time, day) AS metric_time
      , SUM(subq_26.bookings) AS bookings_1_month_ago
    FROM (
      -- Date Spine
      SELECT
        ds AS metric_time
      FROM ***************************.mf_time_spine subq_28
      GROUP BY
        metric_time
    ) subq_27
    INNER JOIN (
      -- Read Elements From Data Source 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        ds AS metric_time
        , 1 AS bookings
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_bookings
      ) bookings_source_src_10001
    ) subq_26
    ON
      DATE_SUB(CAST(subq_27.metric_time AS DATETIME), INTERVAL 1 month) = subq_26.metric_time
    GROUP BY
      metric_time
  ) subq_32
  ON
    (
      subq_24.metric_time = subq_32.metric_time
    ) OR (
      (subq_24.metric_time IS NULL) AND (subq_32.metric_time IS NULL)
    )
) subq_33
