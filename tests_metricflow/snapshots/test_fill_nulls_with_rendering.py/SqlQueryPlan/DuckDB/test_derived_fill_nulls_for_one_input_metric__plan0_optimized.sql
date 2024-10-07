-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0_for_non_offset
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_21.metric_time__day, subq_28.metric_time__day) AS metric_time__day
    , COALESCE(MAX(subq_21.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , MAX(subq_28.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_19.ds AS metric_time__day
        , subq_17.bookings AS bookings
      FROM ***************************.mf_time_spine subq_19
      LEFT OUTER JOIN (
        -- Aggregate Measures
        SELECT
          metric_time__day
          , SUM(bookings) AS bookings
        FROM (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['bookings', 'metric_time__day']
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_16
        GROUP BY
          metric_time__day
      ) subq_17
      ON
        subq_19.ds = subq_17.metric_time__day
    ) subq_20
  ) subq_21
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_25.ds AS metric_time__day
      , SUM(subq_23.bookings) AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine subq_25
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_23
    ON
      subq_25.ds - INTERVAL 14 day = subq_23.metric_time__day
    GROUP BY
      subq_25.ds
  ) subq_28
  ON
    subq_21.metric_time__day = subq_28.metric_time__day
  GROUP BY
    COALESCE(subq_21.metric_time__day, subq_28.metric_time__day)
) subq_29
