test_name: test_duplicate_dimension
test_filename: test_alias_rendering.py
docstring:
  Tests querying the same dimension but with an alias.
sql_engine: DuckDB
---
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Order By ['bookings', 'booking__is_instant', 'booking__is_instant']
-- Change Column Aliases
-- Write to DataTable
SELECT
  booking__is_instant
  , booking__is_instant AS aliased_is_instant
  , SUM(__bookings) AS bookings
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Select: ['__bookings', 'booking__is_instant']
  -- Select: ['__bookings', 'booking__is_instant']
  SELECT
    is_instant AS booking__is_instant
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_11
GROUP BY
  booking__is_instant
ORDER BY bookings, booking__is_instant, booking__is_instant
