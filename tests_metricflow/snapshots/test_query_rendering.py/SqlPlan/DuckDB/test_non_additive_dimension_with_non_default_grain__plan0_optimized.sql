test_name: test_non_additive_dimension_with_non_default_grain
test_filename: test_query_rendering.py
docstring:
  Tests querying a metric with a non-additive agg_time_dimension that has non-default granularity.
sql_engine: DuckDB
---
-- Join on MIN(ds_month) and [] grouping by None
-- Pass Only Elements: ['total_account_balance_first_day_of_month',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(nr_subq_6.total_account_balance_first_day_of_month) AS total_account_balance_first_day_of_month
FROM (
  -- Read Elements From Semantic Model 'accounts_source'
  -- Metric Time Dimension 'ds_month'
  SELECT
    DATE_TRUNC('month', ds_month) AS ds_month__month
    , account_balance AS total_account_balance_first_day_of_month
  FROM ***************************.fct_accounts accounts_source_src_28000
) nr_subq_6
INNER JOIN (
  -- Read Elements From Semantic Model 'accounts_source'
  -- Metric Time Dimension 'ds_month'
  -- Filter row on MIN(ds_month__month)
  SELECT
    MIN(DATE_TRUNC('month', ds_month)) AS ds_month__month__complete
  FROM ***************************.fct_accounts accounts_source_src_28000
) nr_subq_8
ON
  nr_subq_6.ds_month__month = nr_subq_8.ds_month__month__complete
