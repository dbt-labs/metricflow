test_name: test_aliased_item_with_non_aliased_order_by
test_filename: test_alias_rendering.py
docstring:
  Tests querying an item with an alias, but not specifying the alias in the order-by.
sql_engine: DuckDB
---
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Order By ['booking__is_instant']
-- Change Column Aliases
-- Write to DataTable
SELECT
  booking__is_instant AS aliased_is_instant
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
ORDER BY aliased_is_instant
