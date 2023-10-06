-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings - bookings_at_start_of_month AS bookings_growth_since_start_of_month
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_18.metric_time__day, subq_26.metric_time__day) AS metric_time__day
    , subq_18.bookings AS bookings
    , subq_26.bookings_at_start_of_month AS bookings_at_start_of_month
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['bookings', 'metric_time__day']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_16
    GROUP BY
      metric_time__day
  ) subq_18
  INNER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_22.ds AS metric_time__day
      , SUM(subq_20.bookings) AS bookings_at_start_of_month
    FROM ***************************.mf_time_spine subq_22
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_20
    ON
      DATE_TRUNC('month', subq_22.ds) = subq_20.metric_time__day
    GROUP BY
      subq_22.ds
  ) subq_26
  ON
    (
      subq_18.metric_time__day = subq_26.metric_time__day
    ) OR (
      (
        subq_18.metric_time__day IS NULL
      ) AND (
        subq_26.metric_time__day IS NULL
      )
    )
) subq_27
