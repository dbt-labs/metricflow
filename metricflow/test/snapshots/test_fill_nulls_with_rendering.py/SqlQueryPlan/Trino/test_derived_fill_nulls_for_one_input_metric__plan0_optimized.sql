-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings_fill_nulls_with_0 - bookings_2_weeks_ago AS bookings_growth_2_weeks_fill_nulls_with_0_for_non_offset
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_24.metric_time__day, subq_32.metric_time__day) AS metric_time__day
    , COALESCE(MAX(subq_24.bookings_fill_nulls_with_0), 0) AS bookings_fill_nulls_with_0
    , MAX(subq_32.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_22.ds AS metric_time__day
        , subq_20.bookings AS bookings
      FROM ***************************.mf_time_spine subq_22
      LEFT OUTER JOIN (
        -- Aggregate Measures
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
        ) subq_19
        GROUP BY
          metric_time__day
      ) subq_20
      ON
        subq_22.ds = subq_20.metric_time__day
    ) subq_23
  ) subq_24
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_28.ds AS metric_time__day
      , SUM(subq_26.bookings) AS bookings_2_weeks_ago
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
      DATE_ADD('day', -14, subq_28.ds) = subq_26.metric_time__day
    GROUP BY
      subq_28.ds
  ) subq_32
  ON
    subq_24.metric_time__day = subq_32.metric_time__day
  GROUP BY
    COALESCE(subq_24.metric_time__day, subq_32.metric_time__day)
) subq_33
