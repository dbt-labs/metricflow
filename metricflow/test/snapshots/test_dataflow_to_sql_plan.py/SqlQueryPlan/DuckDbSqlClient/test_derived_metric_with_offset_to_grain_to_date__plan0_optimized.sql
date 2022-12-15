-- Compute Metrics via Expressions
SELECT
  metric_time
  , (bookings - bookings_at_start_of_month) / bookings_at_start_of_month AS bookings_growth_since_start_of_month
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_20.metric_time, subq_21.metric_time, subq_22.metric_time) AS metric_time
    , subq_21.bookings AS bookings
    , subq_22.bookings_at_start_of_month AS bookings_at_start_of_month
  FROM (
    -- Date Spine
    SELECT
      ds AS metric_time
    FROM ***************************.mf_time_spine subq_20
  ) subq_20
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
    ) subq_14
    GROUP BY
      metric_time
  ) subq_21
  ON
    subq_20.metric_time = subq_21.metric_time
  INNER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time
      , SUM(bookings) AS bookings_at_start_of_month
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
  ) subq_22
  ON
    DATE_TRUNC('month', COALESCE(subq_20.metric_time, subq_21.metric_time)) = subq_22.metric_time
) subq_23
