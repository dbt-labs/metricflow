-- Join on MIN(ds) and [] grouping by ds
SELECT
  subq_3.ds__day AS ds__day
  , subq_3.ds__week AS ds__week
  , subq_3.ds__month AS ds__month
  , subq_3.ds__quarter AS ds__quarter
  , subq_3.ds__year AS ds__year
  , subq_3.ds__extract_year AS ds__extract_year
  , subq_3.ds__extract_quarter AS ds__extract_quarter
  , subq_3.ds__extract_month AS ds__extract_month
  , subq_3.ds__extract_week AS ds__extract_week
  , subq_3.ds__extract_day AS ds__extract_day
  , subq_3.ds__extract_dayofweek AS ds__extract_dayofweek
  , subq_3.ds__extract_dayofyear AS ds__extract_dayofyear
  , subq_3.account__ds__day AS account__ds__day
  , subq_3.account__ds__week AS account__ds__week
  , subq_3.account__ds__month AS account__ds__month
  , subq_3.account__ds__quarter AS account__ds__quarter
  , subq_3.account__ds__year AS account__ds__year
  , subq_3.account__ds__extract_year AS account__ds__extract_year
  , subq_3.account__ds__extract_quarter AS account__ds__extract_quarter
  , subq_3.account__ds__extract_month AS account__ds__extract_month
  , subq_3.account__ds__extract_week AS account__ds__extract_week
  , subq_3.account__ds__extract_day AS account__ds__extract_day
  , subq_3.account__ds__extract_dayofweek AS account__ds__extract_dayofweek
  , subq_3.account__ds__extract_dayofyear AS account__ds__extract_dayofyear
  , subq_3.user AS user
  , subq_3.account__user AS account__user
  , subq_3.account_type AS account_type
  , subq_3.account__account_type AS account__account_type
  , subq_3.account_balance AS account_balance
  , subq_3.total_account_balance_first_day AS total_account_balance_first_day
  , subq_3.current_account_balance_by_user AS current_account_balance_by_user
FROM (
  -- Read Elements From Semantic Model 'accounts_source'
  SELECT
    account_balance
    , account_balance AS total_account_balance_first_day
    , account_balance AS current_account_balance_by_user
    , ds AS ds__day
    , DATE_TRUNC(ds, isoweek) AS ds__week
    , DATE_TRUNC(ds, month) AS ds__month
    , DATE_TRUNC(ds, quarter) AS ds__quarter
    , DATE_TRUNC(ds, year) AS ds__year
    , EXTRACT(YEAR FROM ds) AS ds__extract_year
    , EXTRACT(QUARTER FROM ds) AS ds__extract_quarter
    , EXTRACT(MONTH FROM ds) AS ds__extract_month
    , EXTRACT(WEEK FROM ds) AS ds__extract_week
    , EXTRACT(DAY FROM ds) AS ds__extract_day
    , EXTRACT(DAYOFWEEK FROM ds) AS ds__extract_dayofweek
    , EXTRACT(DAYOFYEAR FROM ds) AS ds__extract_dayofyear
    , account_type
    , ds AS account__ds__day
    , DATE_TRUNC(ds, isoweek) AS account__ds__week
    , DATE_TRUNC(ds, month) AS account__ds__month
    , DATE_TRUNC(ds, quarter) AS account__ds__quarter
    , DATE_TRUNC(ds, year) AS account__ds__year
    , EXTRACT(YEAR FROM ds) AS account__ds__extract_year
    , EXTRACT(QUARTER FROM ds) AS account__ds__extract_quarter
    , EXTRACT(MONTH FROM ds) AS account__ds__extract_month
    , EXTRACT(WEEK FROM ds) AS account__ds__extract_week
    , EXTRACT(DAY FROM ds) AS account__ds__extract_day
    , EXTRACT(DAYOFWEEK FROM ds) AS account__ds__extract_dayofweek
    , EXTRACT(DAYOFYEAR FROM ds) AS account__ds__extract_dayofyear
    , account_type AS account__account_type
    , user_id AS user
    , user_id AS account__user
  FROM ***************************.fct_accounts accounts_source_src_10000
) subq_3
INNER JOIN (
  -- Read Elements From Semantic Model 'accounts_source'
  -- Filter row on MIN(ds__day)
  SELECT
    DATE_TRUNC(ds, isoweek) AS ds__week
    , MIN(ds) AS ds__day__complete
  FROM ***************************.fct_accounts accounts_source_src_10000
  GROUP BY
    ds__week
) subq_5
ON
  subq_3.ds__day = subq_5.ds__day__complete
