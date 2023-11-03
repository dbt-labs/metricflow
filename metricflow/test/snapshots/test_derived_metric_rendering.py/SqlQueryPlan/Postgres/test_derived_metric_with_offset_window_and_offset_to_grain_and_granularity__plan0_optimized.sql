-- Compute Metrics via Expressions
SELECT
  metric_time__year
  , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_24.metric_time__year, subq_32.metric_time__year) AS metric_time__year
    , subq_24.month_start_bookings AS month_start_bookings
    , subq_32.bookings_1_month_ago AS bookings_1_month_ago
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('year', subq_20.ds) AS metric_time__year
      , SUM(subq_18.bookings) AS month_start_bookings
    FROM ***************************.mf_time_spine subq_20
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_18
    ON
      DATE_TRUNC('month', subq_20.ds) = subq_18.metric_time__day
    WHERE DATE_TRUNC('year', subq_20.ds) = subq_20.ds
    GROUP BY
      DATE_TRUNC('year', subq_20.ds)
  ) subq_24
  INNER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC('year', subq_28.ds) AS metric_time__year
      , SUM(subq_26.bookings) AS bookings_1_month_ago
    FROM ***************************.mf_time_spine subq_28
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_26
    ON
      subq_28.ds - MAKE_INTERVAL(months => 1) = subq_26.metric_time__day
    GROUP BY
      DATE_TRUNC('year', subq_28.ds)
  ) subq_32
  ON
    (
      subq_24.metric_time__year = subq_32.metric_time__year
    ) OR (
      (
        subq_24.metric_time__year IS NULL
      ) AND (
        subq_32.metric_time__year IS NULL
      )
    )
) subq_33
