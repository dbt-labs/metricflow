test_name: test_derived_metric
test_filename: test_derived_metric_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , (bookings - ref_bookings) * 1.0 / bookings AS non_referred_bookings_pct
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
  ) subq_16
  GROUP BY
    metric_time__day
) subq_18
