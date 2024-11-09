test_name: test_reduce_sub_query
test_filename: test_rewriting_sub_query_reducer.py
docstring:
  Tests a case where an outer query should be reduced into its inner query with merged LIMIT expressions.
---
-- src3
SELECT
  SUM(src2.bookings) AS bookings
  , src2.ds
FROM (
  -- src2
  SELECT
    src1.bookings
    , src1.ds
  FROM (
    -- src1
    SELECT
      src0.bookings
      , src0.ds
    FROM demo.fct_bookings src0
    LIMIT 2
  ) src1
  WHERE src1.ds >= '2020-01-01'
  LIMIT 1
) src2
WHERE src2.ds <= '2020-01-05'
GROUP BY
  src2.ds
ORDER BY src2.ds
