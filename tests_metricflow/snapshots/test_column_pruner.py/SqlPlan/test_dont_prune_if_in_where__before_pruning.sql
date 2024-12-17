test_name: test_dont_prune_if_in_where
test_filename: test_column_pruner.py
docstring:
  Tests that columns aren't pruned from parent sources if columns are used in a where.
---
-- test0
SELECT
  from_source.col0 AS from_source_col0
FROM (
  -- from_source
  SELECT
    from_source_table.col0
    , from_source_table.col1
    , from_source_table.join_col
  FROM demo.from_source_table from_source_table
) from_source
INNER JOIN (
  -- joined_source
  SELECT
    joined_source_table.col0
    , joined_source_table.col1
    , joined_source_table.join_col
  FROM demo.joined_source_table joined_source_table
) joined_source
ON
  from_source.join_col = joined_source.join_col
WHERE from_source.col1 IS NULL
