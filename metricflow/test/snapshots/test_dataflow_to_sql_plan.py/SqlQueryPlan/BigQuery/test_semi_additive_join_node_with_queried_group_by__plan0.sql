-- Join on MIN(ds) and [] grouping by ds
SELECT
  subq_0.ds__day AS ds__day
  , subq_0.ds__week AS ds__week
  , subq_0.ds__month AS ds__month
  , subq_0.ds__quarter AS ds__quarter
  , subq_0.ds__year AS ds__year
  , subq_0.ds__extract_year AS ds__extract_year
  , subq_0.ds__extract_quarter AS ds__extract_quarter
  , subq_0.ds__extract_month AS ds__extract_month
  , subq_0.ds__extract_week AS ds__extract_week
  , subq_0.ds__extract_day AS ds__extract_day
  , subq_0.ds__extract_dow AS ds__extract_dow
  , subq_0.ds__extract_doy AS ds__extract_doy
  , subq_0.account__ds__day AS account__ds__day
  , subq_0.account__ds__week AS account__ds__week
  , subq_0.account__ds__month AS account__ds__month
  , subq_0.account__ds__quarter AS account__ds__quarter
  , subq_0.account__ds__year AS account__ds__year
  , subq_0.account__ds__extract_year AS account__ds__extract_year
  , subq_0.account__ds__extract_quarter AS account__ds__extract_quarter
  , subq_0.account__ds__extract_month AS account__ds__extract_month
  , subq_0.account__ds__extract_week AS account__ds__extract_week
  , subq_0.account__ds__extract_day AS account__ds__extract_day
  , subq_0.account__ds__extract_dow AS account__ds__extract_dow
  , subq_0.account__ds__extract_doy AS account__ds__extract_doy
  , subq_0.user AS user
  , subq_0.account__user AS account__user
  , subq_0.account_type AS account_type
  , subq_0.account__account_type AS account__account_type
  , subq_0.account_balance AS account_balance
  , subq_0.total_account_balance_first_day AS total_account_balance_first_day
  , subq_0.current_account_balance_by_user AS current_account_balance_by_user
FROM (
  -- Read Elements From Semantic Model 'accounts_source'
  SELECT
    accounts_source_src_10000.account_balance
    , accounts_source_src_10000.account_balance AS total_account_balance_first_day
    , accounts_source_src_10000.account_balance AS current_account_balance_by_user
    , DATE_TRUNC(accounts_source_src_10000.ds, day) AS ds__day
    , DATE_TRUNC(accounts_source_src_10000.ds, isoweek) AS ds__week
    , DATE_TRUNC(accounts_source_src_10000.ds, month) AS ds__month
    , DATE_TRUNC(accounts_source_src_10000.ds, quarter) AS ds__quarter
    , DATE_TRUNC(accounts_source_src_10000.ds, year) AS ds__year
    , EXTRACT(year FROM accounts_source_src_10000.ds) AS ds__extract_year
    , EXTRACT(quarter FROM accounts_source_src_10000.ds) AS ds__extract_quarter
    , EXTRACT(month FROM accounts_source_src_10000.ds) AS ds__extract_month
    , EXTRACT(isoweek FROM accounts_source_src_10000.ds) AS ds__extract_week
    , EXTRACT(day FROM accounts_source_src_10000.ds) AS ds__extract_day
    , EXTRACT(dayofweek FROM accounts_source_src_10000.ds) AS ds__extract_dow
    , EXTRACT(dayofyear FROM accounts_source_src_10000.ds) AS ds__extract_doy
    , accounts_source_src_10000.account_type
    , DATE_TRUNC(accounts_source_src_10000.ds, day) AS account__ds__day
    , DATE_TRUNC(accounts_source_src_10000.ds, isoweek) AS account__ds__week
    , DATE_TRUNC(accounts_source_src_10000.ds, month) AS account__ds__month
    , DATE_TRUNC(accounts_source_src_10000.ds, quarter) AS account__ds__quarter
    , DATE_TRUNC(accounts_source_src_10000.ds, year) AS account__ds__year
    , EXTRACT(year FROM accounts_source_src_10000.ds) AS account__ds__extract_year
    , EXTRACT(quarter FROM accounts_source_src_10000.ds) AS account__ds__extract_quarter
    , EXTRACT(month FROM accounts_source_src_10000.ds) AS account__ds__extract_month
    , EXTRACT(isoweek FROM accounts_source_src_10000.ds) AS account__ds__extract_week
    , EXTRACT(day FROM accounts_source_src_10000.ds) AS account__ds__extract_day
    , EXTRACT(dayofweek FROM accounts_source_src_10000.ds) AS account__ds__extract_dow
    , EXTRACT(dayofyear FROM accounts_source_src_10000.ds) AS account__ds__extract_doy
    , accounts_source_src_10000.account_type AS account__account_type
    , accounts_source_src_10000.user_id AS user
    , accounts_source_src_10000.user_id AS account__user
  FROM ***************************.fct_accounts accounts_source_src_10000
) subq_0
INNER JOIN (
  -- Filter row on MIN(ds__day)
  SELECT
    subq_1.ds__week
    , MIN(subq_1.ds__day) AS ds__day__complete
  FROM (
    -- Read Elements From Semantic Model 'accounts_source'
    SELECT
      accounts_source_src_10000.account_balance
      , accounts_source_src_10000.account_balance AS total_account_balance_first_day
      , accounts_source_src_10000.account_balance AS current_account_balance_by_user
      , DATE_TRUNC(accounts_source_src_10000.ds, day) AS ds__day
      , DATE_TRUNC(accounts_source_src_10000.ds, isoweek) AS ds__week
      , DATE_TRUNC(accounts_source_src_10000.ds, month) AS ds__month
      , DATE_TRUNC(accounts_source_src_10000.ds, quarter) AS ds__quarter
      , DATE_TRUNC(accounts_source_src_10000.ds, year) AS ds__year
      , EXTRACT(year FROM accounts_source_src_10000.ds) AS ds__extract_year
      , EXTRACT(quarter FROM accounts_source_src_10000.ds) AS ds__extract_quarter
      , EXTRACT(month FROM accounts_source_src_10000.ds) AS ds__extract_month
      , EXTRACT(isoweek FROM accounts_source_src_10000.ds) AS ds__extract_week
      , EXTRACT(day FROM accounts_source_src_10000.ds) AS ds__extract_day
      , EXTRACT(dayofweek FROM accounts_source_src_10000.ds) AS ds__extract_dow
      , EXTRACT(dayofyear FROM accounts_source_src_10000.ds) AS ds__extract_doy
      , accounts_source_src_10000.account_type
      , DATE_TRUNC(accounts_source_src_10000.ds, day) AS account__ds__day
      , DATE_TRUNC(accounts_source_src_10000.ds, isoweek) AS account__ds__week
      , DATE_TRUNC(accounts_source_src_10000.ds, month) AS account__ds__month
      , DATE_TRUNC(accounts_source_src_10000.ds, quarter) AS account__ds__quarter
      , DATE_TRUNC(accounts_source_src_10000.ds, year) AS account__ds__year
      , EXTRACT(year FROM accounts_source_src_10000.ds) AS account__ds__extract_year
      , EXTRACT(quarter FROM accounts_source_src_10000.ds) AS account__ds__extract_quarter
      , EXTRACT(month FROM accounts_source_src_10000.ds) AS account__ds__extract_month
      , EXTRACT(isoweek FROM accounts_source_src_10000.ds) AS account__ds__extract_week
      , EXTRACT(day FROM accounts_source_src_10000.ds) AS account__ds__extract_day
      , EXTRACT(dayofweek FROM accounts_source_src_10000.ds) AS account__ds__extract_dow
      , EXTRACT(dayofyear FROM accounts_source_src_10000.ds) AS account__ds__extract_doy
      , accounts_source_src_10000.account_type AS account__account_type
      , accounts_source_src_10000.user_id AS user
      , accounts_source_src_10000.user_id AS account__user
    FROM ***************************.fct_accounts accounts_source_src_10000
  ) subq_1
  GROUP BY
    ds__week
) subq_2
ON
  subq_0.ds__day = subq_2.ds__day__complete
