test_name: test_semi_additive_join_node
test_filename: test_dataflow_to_sql_plan.py
docstring:
  Tests converting a dataflow plan to a SQL query plan using a SemiAdditiveJoinNode.
sql_engine: DuckDB
---
-- Join on MIN(ds) and [] grouping by None
SELECT
  subq_3.ds__day AS ds__day
  , subq_3.ds__week AS ds__week
  , subq_3.ds__month AS ds__month
  , subq_3.ds__quarter AS ds__quarter
  , subq_3.ds__year AS ds__year
  , subq_3.ds__extract_year AS ds__extract_year
  , subq_3.ds__extract_quarter AS ds__extract_quarter
  , subq_3.ds__extract_month AS ds__extract_month
  , subq_3.ds__extract_day AS ds__extract_day
  , subq_3.ds__extract_dow AS ds__extract_dow
  , subq_3.ds__extract_doy AS ds__extract_doy
  , subq_3.ds_month__month AS ds_month__month
  , subq_3.ds_month__quarter AS ds_month__quarter
  , subq_3.ds_month__year AS ds_month__year
  , subq_3.ds_month__extract_year AS ds_month__extract_year
  , subq_3.ds_month__extract_quarter AS ds_month__extract_quarter
  , subq_3.ds_month__extract_month AS ds_month__extract_month
  , subq_3.account__ds__day AS account__ds__day
  , subq_3.account__ds__week AS account__ds__week
  , subq_3.account__ds__month AS account__ds__month
  , subq_3.account__ds__quarter AS account__ds__quarter
  , subq_3.account__ds__year AS account__ds__year
  , subq_3.account__ds__extract_year AS account__ds__extract_year
  , subq_3.account__ds__extract_quarter AS account__ds__extract_quarter
  , subq_3.account__ds__extract_month AS account__ds__extract_month
  , subq_3.account__ds__extract_day AS account__ds__extract_day
  , subq_3.account__ds__extract_dow AS account__ds__extract_dow
  , subq_3.account__ds__extract_doy AS account__ds__extract_doy
  , subq_3.account__ds_month__month AS account__ds_month__month
  , subq_3.account__ds_month__quarter AS account__ds_month__quarter
  , subq_3.account__ds_month__year AS account__ds_month__year
  , subq_3.account__ds_month__extract_year AS account__ds_month__extract_year
  , subq_3.account__ds_month__extract_quarter AS account__ds_month__extract_quarter
  , subq_3.account__ds_month__extract_month AS account__ds_month__extract_month
  , subq_3.user AS user
  , subq_3.account__user AS account__user
  , subq_3.account_type AS account_type
  , subq_3.account__account_type AS account__account_type
  , subq_3.__account_balance AS __account_balance
  , subq_3.__total_account_balance_first_day AS __total_account_balance_first_day
  , subq_3.__current_account_balance_by_user AS __current_account_balance_by_user
  , subq_3.__total_account_balance_first_day_of_month AS __total_account_balance_first_day_of_month
FROM (
  -- Read Elements From Semantic Model 'accounts_source'
  SELECT
    account_balance AS __account_balance
    , account_balance AS __total_account_balance_first_day
    , account_balance AS __current_account_balance_by_user
    , account_balance AS __total_account_balance_first_day_of_month
    , DATE_TRUNC('day', ds) AS ds__day
    , DATE_TRUNC('week', ds) AS ds__week
    , DATE_TRUNC('month', ds) AS ds__month
    , DATE_TRUNC('quarter', ds) AS ds__quarter
    , DATE_TRUNC('year', ds) AS ds__year
    , EXTRACT(year FROM ds) AS ds__extract_year
    , EXTRACT(quarter FROM ds) AS ds__extract_quarter
    , EXTRACT(month FROM ds) AS ds__extract_month
    , EXTRACT(day FROM ds) AS ds__extract_day
    , EXTRACT(isodow FROM ds) AS ds__extract_dow
    , EXTRACT(doy FROM ds) AS ds__extract_doy
    , DATE_TRUNC('month', ds_month) AS ds_month__month
    , DATE_TRUNC('quarter', ds_month) AS ds_month__quarter
    , DATE_TRUNC('year', ds_month) AS ds_month__year
    , EXTRACT(year FROM ds_month) AS ds_month__extract_year
    , EXTRACT(quarter FROM ds_month) AS ds_month__extract_quarter
    , EXTRACT(month FROM ds_month) AS ds_month__extract_month
    , account_type
    , DATE_TRUNC('day', ds) AS account__ds__day
    , DATE_TRUNC('week', ds) AS account__ds__week
    , DATE_TRUNC('month', ds) AS account__ds__month
    , DATE_TRUNC('quarter', ds) AS account__ds__quarter
    , DATE_TRUNC('year', ds) AS account__ds__year
    , EXTRACT(year FROM ds) AS account__ds__extract_year
    , EXTRACT(quarter FROM ds) AS account__ds__extract_quarter
    , EXTRACT(month FROM ds) AS account__ds__extract_month
    , EXTRACT(day FROM ds) AS account__ds__extract_day
    , EXTRACT(isodow FROM ds) AS account__ds__extract_dow
    , EXTRACT(doy FROM ds) AS account__ds__extract_doy
    , DATE_TRUNC('month', ds_month) AS account__ds_month__month
    , DATE_TRUNC('quarter', ds_month) AS account__ds_month__quarter
    , DATE_TRUNC('year', ds_month) AS account__ds_month__year
    , EXTRACT(year FROM ds_month) AS account__ds_month__extract_year
    , EXTRACT(quarter FROM ds_month) AS account__ds_month__extract_quarter
    , EXTRACT(month FROM ds_month) AS account__ds_month__extract_month
    , account_type AS account__account_type
    , user_id AS user
    , user_id AS account__user
  FROM ***************************.fct_accounts accounts_source_src_28000
) subq_3
INNER JOIN (
  -- Read Elements From Semantic Model 'accounts_source'
  -- Filter row on MIN(ds__day)
  SELECT
    MIN(DATE_TRUNC('day', ds)) AS ds__day__complete
  FROM ***************************.fct_accounts accounts_source_src_28000
) subq_5
ON
  subq_3.ds__day = subq_5.ds__day__complete
