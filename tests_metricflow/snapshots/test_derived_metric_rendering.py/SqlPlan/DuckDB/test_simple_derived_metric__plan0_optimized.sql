test_name: test_simple_derived_metric
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  alias_1 + alias_2 AS test_simple_derived_metric
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings', 'referred_bookings']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    SUM(1) AS alias_1
    , alias_2
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_15
