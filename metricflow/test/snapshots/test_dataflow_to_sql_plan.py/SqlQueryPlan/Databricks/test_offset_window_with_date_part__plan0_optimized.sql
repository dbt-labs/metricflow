-- Compute Metrics via Expressions
SELECT
  metric_time__extract_dow
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_18.metric_time__extract_dow, subq_26.metric_time__extract_dow) AS metric_time__extract_dow
    , subq_18.bookings AS bookings
    , subq_26.bookings_2_weeks_ago AS bookings_2_weeks_ago
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__extract_dow
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['bookings', 'metric_time__extract_dow']
      SELECT
        EXTRACT(dow FROM ds) AS metric_time__extract_dow
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_16
    GROUP BY
      metric_time__extract_dow
  ) subq_18
  INNER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__extract_dow']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      EXTRACT(dow FROM subq_22.ds) AS metric_time__extract_dow
      , SUM(subq_20.bookings) AS bookings_2_weeks_ago
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
      DATEADD(day, -14, subq_22.ds) = subq_20.metric_time__day
    GROUP BY
      EXTRACT(dow FROM subq_22.ds)
  ) subq_26
  ON
    (
      subq_18.metric_time__extract_dow = subq_26.metric_time__extract_dow
    ) OR (
      (
        subq_18.metric_time__extract_dow IS NULL
      ) AND (
        subq_26.metric_time__extract_dow IS NULL
      )
    )
) subq_27
