test_name: test_prune_with_str_expr
test_filename: test_column_pruner.py
docstring:
  Tests that columns are from parent sources if there's a string expression in the select with known cols.
---
-- test0
SELECT
  from_source.col0 AS some_string_expr
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
    joined_source_table.col0
    , joined_source_table.join_col
  FROM demo.joined_source_table joined_source_table
) joined_source
ON
  from_source.join_col = joined_source.join_col
