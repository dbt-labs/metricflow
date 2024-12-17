test_name: test_prune_grandparents_in_join_query
test_filename: test_column_pruner.py
docstring:
  Tests pruning grandparents of a join query.
---
-- 4
SELECT
  col0 AS col0
FROM demo.from_source_table src3
INNER JOIN (
  -- src1
  SELECT
    src1.col0
    , src1.join_col
  FROM (
    -- src0
    SELECT
      src0.col0
      , src0.col1
      , src0.join_col
    FROM demo.src0 src0
  ) src1
) src4
ON
  src3.join_col = src4.join_col
