test_name: test_semi_additive_join_node_with_grouping
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan using a SemiAdditiveJoinNode with a window_grouping.
sql_engine: Clickhouse
---
-- Join on MAX(ds) and ['user'] grouping by None
SELECT
  subq_0.ds__day AS ds__day
  , subq_0.ds__week AS ds__week
  , subq_0.ds__month AS ds__month
  , subq_0.ds__quarter AS ds__quarter
  , subq_0.ds__year AS ds__year
  , subq_0.ds__extract_year AS ds__extract_year
  , subq_0.ds__extract_quarter AS ds__extract_quarter
  , subq_0.ds__extract_month AS ds__extract_month
  , subq_0.ds__extract_day AS ds__extract_day
  , subq_0.ds__extract_dow AS ds__extract_dow
  , subq_0.ds__extract_doy AS ds__extract_doy
  , subq_0.ds_month__month AS ds_month__month
  , subq_0.ds_month__quarter AS ds_month__quarter
  , subq_0.ds_month__year AS ds_month__year
  , subq_0.ds_month__extract_year AS ds_month__extract_year
  , subq_0.ds_month__extract_quarter AS ds_month__extract_quarter
  , subq_0.ds_month__extract_month AS ds_month__extract_month
  , subq_0.account__ds__day AS account__ds__day
  , subq_0.account__ds__week AS account__ds__week
  , subq_0.account__ds__month AS account__ds__month
  , subq_0.account__ds__quarter AS account__ds__quarter
  , subq_0.account__ds__year AS account__ds__year
  , subq_0.account__ds__extract_year AS account__ds__extract_year
  , subq_0.account__ds__extract_quarter AS account__ds__extract_quarter
  , subq_0.account__ds__extract_month AS account__ds__extract_month
  , subq_0.account__ds__extract_day AS account__ds__extract_day
  , subq_0.account__ds__extract_dow AS account__ds__extract_dow
  , subq_0.account__ds__extract_doy AS account__ds__extract_doy
  , subq_0.account__ds_month__month AS account__ds_month__month
  , subq_0.account__ds_month__quarter AS account__ds_month__quarter
  , subq_0.account__ds_month__year AS account__ds_month__year
  , subq_0.account__ds_month__extract_year AS account__ds_month__extract_year
  , subq_0.account__ds_month__extract_quarter AS account__ds_month__extract_quarter
  , subq_0.account__ds_month__extract_month AS account__ds_month__extract_month
  , subq_0.user AS user
  , subq_0.account__user AS account__user
  , subq_0.account_type AS account_type
  , subq_0.account__account_type AS account__account_type
  , subq_0.account_balance AS account_balance
  , subq_0.total_account_balance_first_day AS total_account_balance_first_day
  , subq_0.current_account_balance_by_user AS current_account_balance_by_user
  , subq_0.total_account_balance_first_day_of_month AS total_account_balance_first_day_of_month
FROM (
  -- Read Elements From Semantic Model 'accounts_source'
  SELECT
    accounts_source_src_28000.account_balance
    , accounts_source_src_28000.account_balance AS total_account_balance_first_day
    , accounts_source_src_28000.account_balance AS current_account_balance_by_user
    , accounts_source_src_28000.account_balance AS total_account_balance_first_day_of_month
    , date_trunc('day', accounts_source_src_28000.ds) AS ds__day
    , date_trunc('week', accounts_source_src_28000.ds) AS ds__week
    , date_trunc('month', accounts_source_src_28000.ds) AS ds__month
    , date_trunc('quarter', accounts_source_src_28000.ds) AS ds__quarter
    , date_trunc('year', accounts_source_src_28000.ds) AS ds__year
    , toYear(accounts_source_src_28000.ds) AS ds__extract_year
    , toQuarter(accounts_source_src_28000.ds) AS ds__extract_quarter
    , toMonth(accounts_source_src_28000.ds) AS ds__extract_month
    , toDayOfMonth(accounts_source_src_28000.ds) AS ds__extract_day
    , toDayOfWeek(accounts_source_src_28000.ds) AS ds__extract_dow
    , toDayOfYear(accounts_source_src_28000.ds) AS ds__extract_doy
    , date_trunc('month', accounts_source_src_28000.ds_month) AS ds_month__month
    , date_trunc('quarter', accounts_source_src_28000.ds_month) AS ds_month__quarter
    , date_trunc('year', accounts_source_src_28000.ds_month) AS ds_month__year
    , toYear(accounts_source_src_28000.ds_month) AS ds_month__extract_year
    , toQuarter(accounts_source_src_28000.ds_month) AS ds_month__extract_quarter
    , toMonth(accounts_source_src_28000.ds_month) AS ds_month__extract_month
    , accounts_source_src_28000.account_type
    , date_trunc('day', accounts_source_src_28000.ds) AS account__ds__day
    , date_trunc('week', accounts_source_src_28000.ds) AS account__ds__week
    , date_trunc('month', accounts_source_src_28000.ds) AS account__ds__month
    , date_trunc('quarter', accounts_source_src_28000.ds) AS account__ds__quarter
    , date_trunc('year', accounts_source_src_28000.ds) AS account__ds__year
    , toYear(accounts_source_src_28000.ds) AS account__ds__extract_year
    , toQuarter(accounts_source_src_28000.ds) AS account__ds__extract_quarter
    , toMonth(accounts_source_src_28000.ds) AS account__ds__extract_month
    , toDayOfMonth(accounts_source_src_28000.ds) AS account__ds__extract_day
    , toDayOfWeek(accounts_source_src_28000.ds) AS account__ds__extract_dow
    , toDayOfYear(accounts_source_src_28000.ds) AS account__ds__extract_doy
    , date_trunc('month', accounts_source_src_28000.ds_month) AS account__ds_month__month
    , date_trunc('quarter', accounts_source_src_28000.ds_month) AS account__ds_month__quarter
    , date_trunc('year', accounts_source_src_28000.ds_month) AS account__ds_month__year
    , toYear(accounts_source_src_28000.ds_month) AS account__ds_month__extract_year
    , toQuarter(accounts_source_src_28000.ds_month) AS account__ds_month__extract_quarter
    , toMonth(accounts_source_src_28000.ds_month) AS account__ds_month__extract_month
    , accounts_source_src_28000.account_type AS account__account_type
    , accounts_source_src_28000.user_id AS user
    , accounts_source_src_28000.user_id AS account__user
  FROM ***************************.fct_accounts accounts_source_src_28000
) subq_0
INNER JOIN (
  -- Filter row on MAX(ds__day)
  SELECT
    subq_1.user
    , MAX(subq_1.ds__day) AS ds__day__complete
  FROM (
    -- Read Elements From Semantic Model 'accounts_source'
    SELECT
      accounts_source_src_28000.account_balance
      , accounts_source_src_28000.account_balance AS total_account_balance_first_day
      , accounts_source_src_28000.account_balance AS current_account_balance_by_user
      , accounts_source_src_28000.account_balance AS total_account_balance_first_day_of_month
      , date_trunc('day', accounts_source_src_28000.ds) AS ds__day
      , date_trunc('week', accounts_source_src_28000.ds) AS ds__week
      , date_trunc('month', accounts_source_src_28000.ds) AS ds__month
      , date_trunc('quarter', accounts_source_src_28000.ds) AS ds__quarter
      , date_trunc('year', accounts_source_src_28000.ds) AS ds__year
      , toYear(accounts_source_src_28000.ds) AS ds__extract_year
      , toQuarter(accounts_source_src_28000.ds) AS ds__extract_quarter
      , toMonth(accounts_source_src_28000.ds) AS ds__extract_month
      , toDayOfMonth(accounts_source_src_28000.ds) AS ds__extract_day
      , toDayOfWeek(accounts_source_src_28000.ds) AS ds__extract_dow
      , toDayOfYear(accounts_source_src_28000.ds) AS ds__extract_doy
      , date_trunc('month', accounts_source_src_28000.ds_month) AS ds_month__month
      , date_trunc('quarter', accounts_source_src_28000.ds_month) AS ds_month__quarter
      , date_trunc('year', accounts_source_src_28000.ds_month) AS ds_month__year
      , toYear(accounts_source_src_28000.ds_month) AS ds_month__extract_year
      , toQuarter(accounts_source_src_28000.ds_month) AS ds_month__extract_quarter
      , toMonth(accounts_source_src_28000.ds_month) AS ds_month__extract_month
      , accounts_source_src_28000.account_type
      , date_trunc('day', accounts_source_src_28000.ds) AS account__ds__day
      , date_trunc('week', accounts_source_src_28000.ds) AS account__ds__week
      , date_trunc('month', accounts_source_src_28000.ds) AS account__ds__month
      , date_trunc('quarter', accounts_source_src_28000.ds) AS account__ds__quarter
      , date_trunc('year', accounts_source_src_28000.ds) AS account__ds__year
      , toYear(accounts_source_src_28000.ds) AS account__ds__extract_year
      , toQuarter(accounts_source_src_28000.ds) AS account__ds__extract_quarter
      , toMonth(accounts_source_src_28000.ds) AS account__ds__extract_month
      , toDayOfMonth(accounts_source_src_28000.ds) AS account__ds__extract_day
      , toDayOfWeek(accounts_source_src_28000.ds) AS account__ds__extract_dow
      , toDayOfYear(accounts_source_src_28000.ds) AS account__ds__extract_doy
      , date_trunc('month', accounts_source_src_28000.ds_month) AS account__ds_month__month
      , date_trunc('quarter', accounts_source_src_28000.ds_month) AS account__ds_month__quarter
      , date_trunc('year', accounts_source_src_28000.ds_month) AS account__ds_month__year
      , toYear(accounts_source_src_28000.ds_month) AS account__ds_month__extract_year
      , toQuarter(accounts_source_src_28000.ds_month) AS account__ds_month__extract_quarter
      , toMonth(accounts_source_src_28000.ds_month) AS account__ds_month__extract_month
      , accounts_source_src_28000.account_type AS account__account_type
      , accounts_source_src_28000.user_id AS user
      , accounts_source_src_28000.user_id AS account__user
    FROM ***************************.fct_accounts accounts_source_src_28000
  ) subq_1
  GROUP BY
    user
) subq_2
ON
  (
    subq_0.ds__day = subq_2.ds__day__complete
  ) AND (
    subq_0.user = subq_2.user
  )
