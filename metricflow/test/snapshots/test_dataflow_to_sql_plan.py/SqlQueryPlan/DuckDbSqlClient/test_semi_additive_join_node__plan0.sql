-- Join on MIN(ds) and []
SELECT
  subq_1.ds AS ds
  , subq_1.total_account_balance_first_day AS total_account_balance_first_day
FROM (
  -- Pass Only Elements:
  --   ['total_account_balance_first_day', 'ds']
  SELECT
    subq_0.ds
    , subq_0.total_account_balance_first_day
  FROM (
    -- Read Elements From Data Source 'accounts_source'
    SELECT
      accounts_source_src_10000.account_balance
      , accounts_source_src_10000.account_balance AS total_account_balance_first_day
      , accounts_source_src_10000.account_balance AS current_account_balance_by_user
      , accounts_source_src_10000.ds
      , DATE_TRUNC('week', accounts_source_src_10000.ds) AS ds__week
      , DATE_TRUNC('month', accounts_source_src_10000.ds) AS ds__month
      , DATE_TRUNC('quarter', accounts_source_src_10000.ds) AS ds__quarter
      , DATE_TRUNC('year', accounts_source_src_10000.ds) AS ds__year
      , accounts_source_src_10000.user_id AS user
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_accounts
    ) accounts_source_src_10000
  ) subq_0
) subq_1
INNER JOIN (
  -- Filter row on MIN(ds)
  SELECT
    MIN(subq_2.ds) AS ds
  FROM (
    -- Pass Only Elements:
    --   ['total_account_balance_first_day', 'ds']
    SELECT
      subq_0.ds
      , subq_0.total_account_balance_first_day
    FROM (
      -- Read Elements From Data Source 'accounts_source'
      SELECT
        accounts_source_src_10000.account_balance
        , accounts_source_src_10000.account_balance AS total_account_balance_first_day
        , accounts_source_src_10000.account_balance AS current_account_balance_by_user
        , accounts_source_src_10000.ds
        , DATE_TRUNC('week', accounts_source_src_10000.ds) AS ds__week
        , DATE_TRUNC('month', accounts_source_src_10000.ds) AS ds__month
        , DATE_TRUNC('quarter', accounts_source_src_10000.ds) AS ds__quarter
        , DATE_TRUNC('year', accounts_source_src_10000.ds) AS ds__year
        , accounts_source_src_10000.user_id AS user
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_accounts
      ) accounts_source_src_10000
    ) subq_0
  ) subq_2
) subq_2
ON
  subq_1.ds = subq_2.ds
