test_name: test_reduce_sub_query
test_filename: test_rewriting_sub_query_reducer.py
docstring:
  Tests a case where an outer query should be reduced into its inner query with merged LIMIT expressions.
---
-- src1
-- src2
-- src3
SELECT
  SUM(src0.bookings) AS bookings
  , src0.ds
FROM demo.fct_bookings src0
WHERE (src2.ds <= '2020-01-05') AND (src0.ds >= '2020-01-01')
GROUP BY
  src0.ds
ORDER BY ds
LIMIT 1
