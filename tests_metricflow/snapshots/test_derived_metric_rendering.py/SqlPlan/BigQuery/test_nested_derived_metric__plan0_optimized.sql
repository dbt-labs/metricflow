test_name: test_nested_derived_metric
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , non_referred + (instant * 1.0 / bookings) AS instant_plus_non_referred_bookings_pct
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_23.metric_time__day, subq_28.metric_time__day) AS metric_time__day
    , MAX(subq_23.non_referred) AS non_referred
    , MAX(subq_28.instant) AS instant
    , MAX(subq_28.bookings) AS bookings
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , (bookings - ref_bookings) * 1.0 / bookings AS non_referred
    FROM (
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , SUM(referred_bookings) AS ref_bookings
        , SUM(bookings) AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['referred_bookings', 'bookings', 'metric_time__day']
        SELECT
          TIMESTAMP_TRUNC(ds, day) AS metric_time__day
          , 1 AS bookings
          , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_20
      GROUP BY
        metric_time__day
    ) subq_22
  ) subq_23
  FULL OUTER JOIN (
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(instant_bookings) AS instant
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['instant_bookings', 'bookings', 'metric_time__day']
      SELECT
        TIMESTAMP_TRUNC(ds, day) AS metric_time__day
        , 1 AS bookings
        , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_26
    GROUP BY
      metric_time__day
  ) subq_28
  ON
    subq_23.metric_time__day = subq_28.metric_time__day
  GROUP BY
    metric_time__day
) subq_29
