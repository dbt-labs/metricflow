test_name: test_rewrite_order_by_with_a_join_in_parent
test_filename: test_sub_query_reducer.py
docstring:
  Tests rewriting an order by when the parent has a join.
---
-- src2
-- src3
SELECT
  src0.col0 AS col0
  , src1.col1 AS col1
FROM demo.src0 src0
INNER JOIN
  demo.src1 src1
ON
  src0.join_col = src1.join_col
ORDER BY src1.col1
