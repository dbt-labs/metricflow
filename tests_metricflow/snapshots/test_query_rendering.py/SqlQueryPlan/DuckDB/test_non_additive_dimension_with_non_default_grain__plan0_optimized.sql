-- Join on MIN(ds_month) and [] grouping by None
-- Pass Only Elements: ['total_account_balance_first_day_of_month',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(subq_7.total_account_balance_first_day_of_month) AS total_account_balance_first_day_of_month
FROM (
  -- Read Elements From Semantic Model 'accounts_source'
  -- Metric Time Dimension 'ds_month'
  -- Pass Only Elements: ['total_account_balance_first_day_of_month', 'ds_month__month']
  SELECT
    DATE_TRUNC('month', ds_month) AS ds_month__month
    , account_balance AS total_account_balance_first_day_of_month
  FROM ***************************.fct_accounts accounts_source_src_28000
) subq_7
INNER JOIN (
  -- Read Elements From Semantic Model 'accounts_source'
  -- Metric Time Dimension 'ds_month'
  -- Pass Only Elements: ['total_account_balance_first_day_of_month', 'ds_month__month']
  -- Filter row on MIN(ds_month__month)
  SELECT
    MIN(DATE_TRUNC('month', ds_month)) AS ds_month__month__complete
  FROM ***************************.fct_accounts accounts_source_src_28000
) subq_9
ON
  subq_7.ds_month__month = subq_9.ds_month__month__complete
