test_name: test_simple_derived_metric
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  alias_1 + alias_2 + alias_3 + alias_4 AS test_simple_derived_metric
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['__bookings', '__referred_bookings', '__instant_bookings', '__booking_value']
  -- Pass Only Elements: ['__bookings', '__referred_bookings', '__instant_bookings', '__booking_value']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS alias_1
    , SUM(CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END) AS alias_2
    , SUM(CASE WHEN is_instant THEN 1 ELSE 0 END) AS alias_3
    , SUM(booking_value) AS alias_4
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_28
