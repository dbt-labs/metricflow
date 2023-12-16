-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_21.metric_time__day, subq_30.metric_time__day) AS metric_time__day
    , MAX(subq_21.bookings) AS bookings
    , MAX(subq_30.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Constrain Output with WHERE
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
    ) subq_18
    WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    GROUP BY
      metric_time__day
  ) subq_21
  FULL OUTER JOIN (
    -- Constrain Output with WHERE
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings_2_weeks_ago
    FROM (
      -- Join to Time Spine Dataset
      -- Pass Only Elements:
      --   ['bookings', 'metric_time__day']
      SELECT
        subq_25.ds AS metric_time__day
        , subq_23.bookings AS bookings
      FROM ***************************.mf_time_spine subq_25
      INNER JOIN (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_10001
      ) subq_23
      ON
        DATE_ADD('day', -14, subq_25.ds) = subq_23.metric_time__day
    ) subq_27
    WHERE metric_time__day = '2020-01-01' or metric_time__day = '2020-01-14'
    GROUP BY
      metric_time__day
  ) subq_30
  ON
    subq_21.metric_time__day = subq_30.metric_time__day
  GROUP BY
    COALESCE(subq_21.metric_time__day, subq_30.metric_time__day)
) subq_31
