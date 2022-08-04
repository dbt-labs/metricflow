-- Join on MAX(ds) and ['user']
SELECT
  subq_4.ds AS ds
  , subq_4.user AS user
  , subq_4.current_account_balance_by_user AS current_account_balance_by_user
FROM (
  -- Read Elements From Data Source 'accounts_source'
  -- Pass Only Elements:
  --   ['current_account_balance_by_user', 'user', 'ds']
  SELECT
    ds
    , user_id AS user
    , account_balance AS current_account_balance_by_user
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_accounts
  ) accounts_source_src_10000
) subq_4
INNER JOIN (
  -- Read Elements From Data Source 'accounts_source'
  -- Pass Only Elements:
  --   ['current_account_balance_by_user', 'user', 'ds']
  -- Filter row on MAX(ds)
  SELECT
    user_id AS user
    , MAX(ds) AS ds
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_accounts
  ) accounts_source_src_10000
  GROUP BY
    user_id
) subq_5
ON
  (subq_4.ds = subq_5.ds) AND (subq_4.user = subq_5.user)
