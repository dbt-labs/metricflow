-- Compute Metrics via Expressions
SELECT
  metric_time__year
  , month_start_bookings - bookings_1_month_ago AS bookings_month_start_compared_to_1_month_prior
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_24.metric_time__year, subq_32.metric_time__year) AS metric_time__year
    , MAX(subq_24.month_start_bookings) AS month_start_bookings
    , MAX(subq_32.bookings_1_month_ago) AS bookings_1_month_ago
  FROM (
    -- Join to Time Spine Dataset
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC(subq_20.ds, year) AS metric_time__year
      , SUM(subq_18.bookings) AS month_start_bookings
    FROM ***************************.mf_time_spine subq_20
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC(ds, day) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_18
    ON
      DATE_TRUNC(subq_20.ds, month) = subq_18.metric_time__day
    WHERE DATE_TRUNC(subq_20.ds, year) = subq_20.ds
    GROUP BY
      metric_time__year
  ) subq_24
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__year']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      DATE_TRUNC(subq_28.ds, year) AS metric_time__year
      , SUM(subq_26.bookings) AS bookings_1_month_ago
    FROM ***************************.mf_time_spine subq_28
    INNER JOIN (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC(ds, day) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_26
    ON
      DATE_SUB(CAST(subq_28.ds AS DATETIME), INTERVAL 1 month) = subq_26.metric_time__day
    GROUP BY
      metric_time__year
  ) subq_32
  ON
    subq_24.metric_time__year = subq_32.metric_time__year
  GROUP BY
    metric_time__year
) subq_33
