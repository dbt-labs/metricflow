test_name: test_prune_distinct_select
test_filename: test_column_pruner.py
docstring:
  Test that distinct select node shouldn't be pruned.
---
-- test0
SELECT
  a.booking_value
FROM (
  -- test1
  SELECT DISTINCT
    a.booking_value
    , a.bookings
  FROM demo.fct_bookings a
) b
