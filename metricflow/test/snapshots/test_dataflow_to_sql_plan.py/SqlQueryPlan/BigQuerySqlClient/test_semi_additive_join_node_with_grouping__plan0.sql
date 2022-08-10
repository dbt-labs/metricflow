-- Join on MAX(ds) and ['user']
SELECT
  subq_1.ds AS ds
  , subq_1.user AS user
  , subq_1.current_account_balance_by_user AS current_account_balance_by_user
FROM (
  -- Pass Only Elements:
  --   ['current_account_balance_by_user', 'user', 'ds']
  SELECT
    subq_0.ds
    , subq_0.user
    , subq_0.current_account_balance_by_user
  FROM (
    -- Read Elements From Data Source 'accounts_source'
    SELECT
      accounts_source_src_10000.account_balance
      , accounts_source_src_10000.account_balance AS total_account_balance_first_day
      , accounts_source_src_10000.account_balance AS current_account_balance_by_user
      , accounts_source_src_10000.ds
      , DATE_TRUNC(accounts_source_src_10000.ds, isoweek) AS ds__week
      , DATE_TRUNC(accounts_source_src_10000.ds, month) AS ds__month
      , DATE_TRUNC(accounts_source_src_10000.ds, quarter) AS ds__quarter
      , DATE_TRUNC(accounts_source_src_10000.ds, isoyear) AS ds__year
      , accounts_source_src_10000.user_id AS user
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_accounts
    ) accounts_source_src_10000
  ) subq_0
) subq_1
INNER JOIN (
  -- Filter row on MAX(ds)
  SELECT
    subq_2.user
    , MAX(subq_2.ds) AS ds
  FROM (
    -- Pass Only Elements:
    --   ['current_account_balance_by_user', 'user', 'ds']
    SELECT
      subq_0.ds
      , subq_0.user
      , subq_0.current_account_balance_by_user
    FROM (
      -- Read Elements From Data Source 'accounts_source'
      SELECT
        accounts_source_src_10000.account_balance
        , accounts_source_src_10000.account_balance AS total_account_balance_first_day
        , accounts_source_src_10000.account_balance AS current_account_balance_by_user
        , accounts_source_src_10000.ds
        , DATE_TRUNC(accounts_source_src_10000.ds, isoweek) AS ds__week
        , DATE_TRUNC(accounts_source_src_10000.ds, month) AS ds__month
        , DATE_TRUNC(accounts_source_src_10000.ds, quarter) AS ds__quarter
        , DATE_TRUNC(accounts_source_src_10000.ds, isoyear) AS ds__year
        , accounts_source_src_10000.user_id AS user
      FROM (
        -- User Defined SQL Query
        SELECT * FROM ***************************.fct_accounts
      ) accounts_source_src_10000
    ) subq_0
  ) subq_2
  GROUP BY
    subq_2.user
) subq_2
ON
  (subq_1.ds = subq_2.ds) AND (subq_1.user = subq_2.user)
