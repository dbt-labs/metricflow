test_name: test_nested_derived_metric
test_filename: test_derived_metric_rendering.py
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , non_referred + (instant * 1.0 / bookings) AS instant_plus_non_referred_bookings_pct
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_21.metric_time__day, nr_subq_26.metric_time__day) AS metric_time__day
    , MAX(nr_subq_21.non_referred) AS non_referred
    , MAX(nr_subq_26.instant) AS instant
    , MAX(nr_subq_26.bookings) AS bookings
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
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS bookings
          , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) nr_subq_18
      GROUP BY
        metric_time__day
    ) nr_subq_20
  ) nr_subq_21
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
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
        , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_24
    GROUP BY
      metric_time__day
  ) nr_subq_26
  ON
    nr_subq_21.metric_time__day = nr_subq_26.metric_time__day
  GROUP BY
    COALESCE(nr_subq_21.metric_time__day, nr_subq_26.metric_time__day)
) nr_subq_27
