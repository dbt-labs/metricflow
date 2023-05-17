-- Join on MIN(ds) and [] grouping by ds
SELECT
  subq_0.ds AS ds
  , subq_0.ds__week AS ds__week
  , subq_0.ds__month AS ds__month
  , subq_0.ds__quarter AS ds__quarter
  , subq_0.ds__year AS ds__year
  , subq_0.user AS user
  , subq_0.account_type AS account_type
  , subq_0.account_balance AS account_balance
  , subq_0.total_account_balance_first_day AS total_account_balance_first_day
  , subq_0.current_account_balance_by_user AS current_account_balance_by_user
FROM (
  -- Read Elements From Semantic Model 'accounts_source'
  SELECT
    accounts_source_src_10000.account_balance
    , accounts_source_src_10000.account_balance AS total_account_balance_first_day
    , accounts_source_src_10000.account_balance AS current_account_balance_by_user
    , accounts_source_src_10000.ds
    , DATE_TRUNC('week', accounts_source_src_10000.ds) AS ds__week
    , DATE_TRUNC('month', accounts_source_src_10000.ds) AS ds__month
    , DATE_TRUNC('quarter', accounts_source_src_10000.ds) AS ds__quarter
    , DATE_TRUNC('year', accounts_source_src_10000.ds) AS ds__year
    , accounts_source_src_10000.account_type
    , accounts_source_src_10000.user_id AS user
  FROM ***************************.fct_accounts accounts_source_src_10000
) subq_0
INNER JOIN (
  -- Filter row on MIN(ds)
  SELECT
    subq_1.ds__week
    , MIN(subq_1.ds) AS ds__complete
  FROM (
    -- Read Elements From Semantic Model 'accounts_source'
    SELECT
      accounts_source_src_10000.account_balance
      , accounts_source_src_10000.account_balance AS total_account_balance_first_day
      , accounts_source_src_10000.account_balance AS current_account_balance_by_user
      , accounts_source_src_10000.ds
      , DATE_TRUNC('week', accounts_source_src_10000.ds) AS ds__week
      , DATE_TRUNC('month', accounts_source_src_10000.ds) AS ds__month
      , DATE_TRUNC('quarter', accounts_source_src_10000.ds) AS ds__quarter
      , DATE_TRUNC('year', accounts_source_src_10000.ds) AS ds__year
      , accounts_source_src_10000.account_type
      , accounts_source_src_10000.user_id AS user
    FROM ***************************.fct_accounts accounts_source_src_10000
  ) subq_1
  GROUP BY
    subq_1.ds__week
) subq_2
ON
  subq_0.ds = subq_2.ds__complete
