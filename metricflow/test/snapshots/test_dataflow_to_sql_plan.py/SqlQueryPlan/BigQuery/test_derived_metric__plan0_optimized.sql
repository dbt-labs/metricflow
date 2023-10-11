-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , (bookings - ref_bookings) * 1.0 / bookings AS non_referred_bookings_pct
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_15.metric_time__day, subq_20.metric_time__day) AS metric_time__day
    , subq_15.ref_bookings AS ref_bookings
    , subq_20.bookings AS bookings
  FROM (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(referred_bookings) AS ref_bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['referred_bookings', 'metric_time__day']
      SELECT
        DATE_TRUNC(ds, day) AS metric_time__day
        , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_13
    GROUP BY
      metric_time__day
  ) subq_15
  INNER JOIN (
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
        DATE_TRUNC(ds, day) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_10001
    ) subq_18
    GROUP BY
      metric_time__day
  ) subq_20
  ON
    (
      subq_15.metric_time__day = subq_20.metric_time__day
    ) OR (
      (
        subq_15.metric_time__day IS NULL
      ) AND (
        subq_20.metric_time__day IS NULL
      )
    )
) subq_21
