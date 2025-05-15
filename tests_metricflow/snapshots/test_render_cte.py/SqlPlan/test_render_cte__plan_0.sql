test_name: test_render_cte
test_filename: test_render_cte.py
---
-- cte_test
WITH cte_0 AS (
  -- cte_select_0
  SELECT
    cte_source_table_0.col_0
  FROM demo.cte_source_table_0 cte_source_table_0
)

, cte_1 AS (
  -- cte_select_1
  SELECT
    cte_source_table_1.col_1
  FROM demo.cte_source_table_1 cte_source_table_1
)

SELECT
  cte_0.col_0 AS col_0
  , cte_1.col_1 AS col_1
FROM cte_0
LEFT OUTER JOIN
  cte_1
ON
  cte_0.col_0 = cte_1.col_1
