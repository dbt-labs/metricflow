test_name: test_reduce_sub_query
test_filename: test_rewriting_sub_query_reducer.py
docstring:
  Tests a case where an outer query should be reduced into its inner query with merged LIMIT expressions.
sql_engine: ClickHouse
---
SELECT
  SUM(src2.bookings) AS bookings
  , src2.ds
FROM (
  SELECT
    src0.bookings
    , src0.ds
  FROM demo.fct_bookings src0
  WHERE src0.ds >= '2020-01-01'
  LIMIT 1
) src2
WHERE src2.ds <= '2020-01-05'
GROUP BY
  src2.ds
ORDER BY src2.ds
