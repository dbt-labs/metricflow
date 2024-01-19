-- Compute Metrics via Expressions
SELECT
  metric_time__extract_dow
  , bookings - bookings_2_weeks_ago AS bookings_growth_2_weeks
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_18.metric_time__extract_dow, subq_26.metric_time__extract_dow) AS metric_time__extract_dow
    , MAX(subq_18.bookings) AS bookings
    , MAX(subq_26.bookings_2_weeks_ago) AS bookings_2_weeks_ago
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
        IF(EXTRACT(dayofweek FROM ds) = 1, 7, EXTRACT(dayofweek FROM ds) - 1) AS metric_time__extract_dow
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_16
    GROUP BY
      metric_time__extract_dow
  ) subq_18
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements: ['bookings', 'metric_time__extract_dow']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      IF(EXTRACT(dayofweek FROM subq_22.ds) = 1, 7, EXTRACT(dayofweek FROM subq_22.ds) - 1) AS metric_time__extract_dow
      , SUM(subq_20.bookings) AS bookings_2_weeks_ago
    FROM ***************************.mf_time_spine subq_22
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC(ds, day) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_20
    ON
      DATE_SUB(CAST(subq_22.ds AS DATETIME), INTERVAL 14 day) = subq_20.metric_time__day
    GROUP BY
      metric_time__extract_dow
  ) subq_26
  ON
    subq_18.metric_time__extract_dow = subq_26.metric_time__extract_dow
  GROUP BY
    metric_time__extract_dow
) subq_27
