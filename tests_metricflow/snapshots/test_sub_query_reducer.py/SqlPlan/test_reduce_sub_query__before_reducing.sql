test_name: test_reduce_sub_query
test_filename: test_sub_query_reducer.py
docstring:
  Tests a case where an outer query should be reduced into its inner query with merged LIMIT expressions.
---
-- src3
SELECT
  src2.col0
  , src2.col1
FROM (
  -- src2
  SELECT
    src1.col0
    , src1.col1
  FROM (
    -- src1
    SELECT
      src0.col0
      , src0.col1
    FROM demo.from_source_table src0
    LIMIT 2
  ) src1
  LIMIT 1
) src2
ORDER BY src2.col0
