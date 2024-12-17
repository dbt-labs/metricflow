test_name: test_reduce_sub_query
test_filename: test_sub_query_reducer.py
docstring:
  Tests a case where an outer query should be reduced into its inner query with merged LIMIT expressions.
---
-- src1
-- src2
-- src3
SELECT
  src0.col0
  , src0.col1
FROM demo.from_source_table src0
ORDER BY src0.col0
LIMIT 1
