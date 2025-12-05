test_name: test_non_additive_dimension_with_non_default_grain
test_filename: test_query_rendering.py
docstring:
  Tests querying a metric with a non-additive agg_time_dimension that has non-default granularity.
sql_engine: Redshift
---
-- Join on MIN(ds_month) and [] grouping by None
-- Pass Only Elements: ['__total_account_balance_first_day_of_month']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  SUM(subq_11.__total_account_balance_first_day_of_month) AS total_account_balance_first_day_of_month
FROM (
  -- Read Elements From Semantic Model 'accounts_source'
  -- Metric Time Dimension 'ds_month'
  -- Pass Only Elements: ['__total_account_balance_first_day_of_month', 'ds_month__month']
  SELECT
    DATE_TRUNC('month', ds_month) AS ds_month__month
    , account_balance AS __total_account_balance_first_day_of_month
  FROM ***************************.fct_accounts accounts_source_src_28000
) subq_11
INNER JOIN (
  -- Read Elements From Semantic Model 'accounts_source'
  -- Metric Time Dimension 'ds_month'
  -- Pass Only Elements: ['__total_account_balance_first_day_of_month', 'ds_month__month']
  -- Filter row on MIN(ds_month__month)
  SELECT
    MIN(DATE_TRUNC('month', ds_month)) AS ds_month__month__complete
  FROM ***************************.fct_accounts accounts_source_src_28000
) subq_13
ON
  subq_11.ds_month__month = subq_13.ds_month__month__complete
