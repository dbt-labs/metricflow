-- Join on MAX(ds) and ['user'] grouping by None
SELECT
  subq_3.ds AS ds
  , subq_3.ds__week AS ds__week
  , subq_3.ds__month AS ds__month
  , subq_3.ds__quarter AS ds__quarter
  , subq_3.ds__year AS ds__year
  , subq_3.user AS user
  , subq_3.account_type AS account_type
  , subq_3.account_balance AS account_balance
  , subq_3.total_account_balance_first_day AS total_account_balance_first_day
  , subq_3.current_account_balance_by_user AS current_account_balance_by_user
FROM (
  -- Read Elements From Semantic Model 'accounts_source'
  SELECT
    account_balance
    , account_balance AS total_account_balance_first_day
    , account_balance AS current_account_balance_by_user
    , ds
    , DATE_TRUNC(ds, isoweek) AS ds__week
    , DATE_TRUNC(ds, month) AS ds__month
    , DATE_TRUNC(ds, quarter) AS ds__quarter
    , DATE_TRUNC(ds, isoyear) AS ds__year
    , account_type
    , user_id AS user
  FROM ***************************.fct_accounts accounts_source_src_10000
) subq_3
INNER JOIN (
  -- Read Elements From Semantic Model 'accounts_source'
  -- Filter row on MAX(ds)
  SELECT
    user_id AS user
    , MAX(ds) AS ds__complete
  FROM ***************************.fct_accounts accounts_source_src_10000
  GROUP BY
    user
) subq_5
ON
  (subq_3.ds = subq_5.ds__complete) AND (subq_3.user = subq_5.user)
