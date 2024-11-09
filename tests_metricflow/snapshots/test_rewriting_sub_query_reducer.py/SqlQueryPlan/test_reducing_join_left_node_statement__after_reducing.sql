test_name: test_reducing_join_left_node_statement
test_filename: test_rewriting_sub_query_reducer.py
docstring:
  Tests a case where a join query should not reduced an aggregate.
---
-- query
SELECT
  src2.bookings AS bookings
  , src3.listings AS listings
FROM (
  -- src4
  SELECT
    SUM(src4.listings) AS listings
  FROM demo.fct_listings src4
) src2
CROSS JOIN (
  -- src1
  -- src2
  SELECT
    SUM(1) AS bookings
  FROM demo.fct_bookings src0
) src3
