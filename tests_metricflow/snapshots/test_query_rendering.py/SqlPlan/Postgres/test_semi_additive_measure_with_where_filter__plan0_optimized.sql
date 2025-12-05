test_name: test_semi_additive_measure_with_where_filter
test_filename: test_query_rendering.py
docstring:
  Tests querying a semi-additive measure with a where filter.
sql_engine: Postgres
---
-- Join on MAX(ds) and ['user'] grouping by None
-- Pass Only Elements: ['__current_account_balance_by_user', 'user']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_13.user AS user
  , SUM(subq_13.__current_account_balance_by_user) AS current_account_balance_by_user
FROM (
  -- Constrain Output with WHERE
  SELECT
    current_account_balance_by_user AS __current_account_balance_by_user
    , ds__day
    , subq_12.user
  FROM (
    -- Read Elements From Semantic Model 'accounts_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['__current_account_balance_by_user', 'account__account_type', 'ds__day', 'user']
    SELECT
      DATE_TRUNC('day', ds) AS ds__day
      , user_id AS user
      , account_type AS account__account_type
      , account_balance AS current_account_balance_by_user
    FROM ***************************.fct_accounts accounts_source_src_28000
  ) subq_12
  WHERE account__account_type = 'savings'
) subq_13
INNER JOIN (
  -- Constrain Output with WHERE
  -- Filter row on MAX(ds__day)
  SELECT
    subq_12.user
    , MAX(ds__day) AS ds__day__complete
  FROM (
    -- Read Elements From Semantic Model 'accounts_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['__current_account_balance_by_user', 'account__account_type', 'ds__day', 'user']
    SELECT
      DATE_TRUNC('day', ds) AS ds__day
      , user_id AS user
      , account_type AS account__account_type
    FROM ***************************.fct_accounts accounts_source_src_28000
  ) subq_12
  WHERE account__account_type = 'savings'
  GROUP BY
    subq_12.user
) subq_15
ON
  (
    subq_13.ds__day = subq_15.ds__day__complete
  ) AND (
    subq_13.user = subq_15.user
  )
GROUP BY
  subq_13.user
