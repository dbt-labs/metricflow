test_name: test_prune_str_expr
test_filename: test_column_pruner.py
docstring:
  Tests a case where a string expr in a node results in the parent being pruned properly.
---
-- test0
SELECT
  col0 AS from_source_col0
FROM (
  -- from_source
  SELECT
    from_source_table.col0
    , from_source_table.join_col
  FROM demo.from_source_table from_source_table
) from_source
INNER JOIN (
  -- joined_source
  SELECT
    joined_source_table.col2 AS col0
    , joined_source_table.join_col
  FROM demo.joined_source_table joined_source_table
) joined_source
ON
  from_source.join_col = joined_source.join_col
