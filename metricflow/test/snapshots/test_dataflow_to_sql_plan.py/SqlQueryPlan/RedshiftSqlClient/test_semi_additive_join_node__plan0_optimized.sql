-- Join on MIN(ds) and []
SELECT
  subq_4.ds AS ds
  , subq_4.total_account_balance_first_day AS total_account_balance_first_day
FROM (
  -- Read Elements From Data Source 'accounts_source'
  -- Pass Only Elements:
  --   ['total_account_balance_first_day', 'ds']
  SELECT
    ds
    , account_balance AS total_account_balance_first_day
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_accounts
  ) accounts_source_src_10000
) subq_4
INNER JOIN (
  -- Read Elements From Data Source 'accounts_source'
  -- Pass Only Elements:
  --   ['total_account_balance_first_day', 'ds']
  -- Filter row on MIN(ds)
  SELECT
    MIN(ds) AS ds
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_accounts
  ) accounts_source_src_10000
) subq_5
ON
  subq_4.ds = subq_5.ds
