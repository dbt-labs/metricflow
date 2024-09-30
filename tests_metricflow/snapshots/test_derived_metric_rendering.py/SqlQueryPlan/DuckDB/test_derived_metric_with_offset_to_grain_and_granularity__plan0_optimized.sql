-- Compute Metrics via Expressions
SELECT
  metric_time__week
  , bookings - bookings_at_start_of_month AS bookings_growth_since_start_of_month
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_15.metric_time__week, subq_22.metric_time__week) AS metric_time__week
    , MAX(subq_15.bookings) AS bookings
    , MAX(subq_22.bookings_at_start_of_month) AS bookings_at_start_of_month
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__week
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__week']
      SELECT
        DATE_TRUNC('week', ds) AS metric_time__week
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_13
    GROUP BY
      metric_time__week
  ) subq_15
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__week']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('week', subq_19.ds) AS metric_time__week
      , SUM(subq_17.bookings) AS bookings_at_start_of_month
    FROM ***************************.mf_time_spine subq_19
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_17
    ON
      DATE_TRUNC('month', subq_19.ds) = subq_17.metric_time__day
    WHERE DATE_TRUNC('week', subq_19.ds) = subq_19.ds
    GROUP BY
      DATE_TRUNC('week', subq_19.ds)
  ) subq_22
  ON
    subq_15.metric_time__week = subq_22.metric_time__week
  GROUP BY
    COALESCE(subq_15.metric_time__week, subq_22.metric_time__week)
) subq_23
