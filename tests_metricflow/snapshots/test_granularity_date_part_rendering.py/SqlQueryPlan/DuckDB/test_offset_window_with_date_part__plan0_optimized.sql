-- Compute Metrics via Expressions
SELECT
  metric_time__extract_dow
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_15.metric_time__extract_dow, subq_22.metric_time__extract_dow) AS metric_time__extract_dow
    , MAX(subq_15.bookings) AS bookings
    , MAX(subq_22.bookings_2_weeks_ago) AS bookings_2_weeks_ago
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__extract_dow
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__extract_dow']
      SELECT
        EXTRACT(isodow FROM ds) AS metric_time__extract_dow
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_13
    GROUP BY
      metric_time__extract_dow
  ) subq_15
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__extract_dow']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      EXTRACT(isodow FROM subq_19.ds) AS metric_time__extract_dow
      , SUM(subq_17.bookings) AS bookings_2_weeks_ago
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
      subq_19.ds - INTERVAL 14 day = subq_17.metric_time__day
    GROUP BY
      EXTRACT(isodow FROM subq_19.ds)
  ) subq_22
  ON
    subq_15.metric_time__extract_dow = subq_22.metric_time__extract_dow
  GROUP BY
    COALESCE(subq_15.metric_time__extract_dow, subq_22.metric_time__extract_dow)
) subq_23
