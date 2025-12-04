test_name: test_semi_additive_measure_with_where_filter
test_filename: test_query_rendering.py
docstring:
  Tests querying a semi-additive measure with a where filter.
sql_engine: DuckDB
---
-- Join on MAX(ds) and ['user'] grouping by None
-- Pass Only Elements: ['__current_account_balance_by_user', 'user']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_11.user AS user
  , SUM(subq_11.__current_account_balance_by_user) AS current_account_balance_by_user
FROM (
  -- Constrain Output with WHERE
  SELECT
    current_account_balance_by_user AS __current_account_balance_by_user
    , ds__day
    , subq_10.user
  FROM (
    -- Read Elements From Semantic Model 'accounts_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS ds__day
      , user_id AS user
      , account_type AS account__account_type
      , account_balance AS current_account_balance_by_user
    FROM ***************************.fct_accounts accounts_source_src_28000
  ) subq_10
  WHERE account__account_type = 'savings'
) subq_11
INNER JOIN (
  -- Constrain Output with WHERE
  -- Filter row on MAX(ds__day)
  SELECT
    subq_10.user
    , MAX(ds__day) AS ds__day__complete
  FROM (
    -- Read Elements From Semantic Model 'accounts_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS ds__day
      , user_id AS user
      , account_type AS account__account_type
    FROM ***************************.fct_accounts accounts_source_src_28000
  ) subq_10
  WHERE account__account_type = 'savings'
  GROUP BY
    subq_10.user
) subq_13
ON
  (
    subq_11.ds__day = subq_13.ds__day__complete
  ) AND (
    subq_11.user = subq_13.user
  )
GROUP BY
  subq_11.user
