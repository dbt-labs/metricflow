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
    COALESCE(subq_27.metric_time__day, subq_33.metric_time__day) AS metric_time__day
    , MAX(subq_27.non_referred) AS non_referred
    , MAX(subq_33.instant) AS instant
    , MAX(subq_33.bookings) AS bookings
  FROM (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , (bookings - ref_bookings) * 1.0 / bookings AS non_referred
    FROM (
      -- Aggregate Inputs for Simple Metrics
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , SUM(__referred_bookings) AS ref_bookings
        , SUM(__bookings) AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['__referred_bookings', '__bookings', 'metric_time__day']
        -- Pass Only Elements: ['__referred_bookings', '__bookings', 'metric_time__day']
        SELECT
          DATETIME_TRUNC(ds, day) AS metric_time__day
          , 1 AS __bookings
          , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS __referred_bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_24
      GROUP BY
        metric_time__day
    ) subq_26
  ) subq_27
  FULL OUTER JOIN (
    -- Aggregate Inputs for Simple Metrics
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , SUM(__instant_bookings) AS instant
      , SUM(__bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['__instant_bookings', '__bookings', 'metric_time__day']
      -- Pass Only Elements: ['__instant_bookings', '__bookings', 'metric_time__day']
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , 1 AS __bookings
        , CASE WHEN is_instant THEN 1 ELSE 0 END AS __instant_bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_31
    GROUP BY
      metric_time__day
  ) subq_33
  ON
    subq_27.metric_time__day = subq_33.metric_time__day
  GROUP BY
    metric_time__day
) subq_34
