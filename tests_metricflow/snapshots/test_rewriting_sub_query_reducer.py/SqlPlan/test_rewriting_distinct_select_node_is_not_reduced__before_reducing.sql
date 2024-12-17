test_name: test_rewriting_distinct_select_node_is_not_reduced
test_filename: test_rewriting_sub_query_reducer.py
docstring:
  Tests to ensure distinct select node doesn't get overwritten.
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
