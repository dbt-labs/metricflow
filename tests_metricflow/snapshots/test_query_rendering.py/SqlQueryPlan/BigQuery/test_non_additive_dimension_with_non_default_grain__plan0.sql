test_name: test_non_additive_dimension_with_non_default_grain
test_filename: test_query_rendering.py
docstring:
  Tests querying a metric with a non-additive agg_time_dimension that has non-default granularity.
---
-- Compute Metrics via Expressions
SELECT
  subq_6.total_account_balance_first_day_of_month
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_5.total_account_balance_first_day_of_month) AS total_account_balance_first_day_of_month
  FROM (
    -- Pass Only Elements: ['total_account_balance_first_day_of_month',]
    SELECT
      subq_4.total_account_balance_first_day_of_month
    FROM (
      -- Join on MIN(ds_month) and [] grouping by None
      SELECT
        subq_1.ds__day AS ds__day
        , subq_1.ds__week AS ds__week
        , subq_1.ds__month AS ds__month
        , subq_1.ds__quarter AS ds__quarter
        , subq_1.ds__year AS ds__year
        , subq_1.ds__extract_year AS ds__extract_year
        , subq_1.ds__extract_quarter AS ds__extract_quarter
        , subq_1.ds__extract_month AS ds__extract_month
        , subq_1.ds__extract_day AS ds__extract_day
        , subq_1.ds__extract_dow AS ds__extract_dow
        , subq_1.ds__extract_doy AS ds__extract_doy
        , subq_1.ds_month__month AS ds_month__month
        , subq_1.ds_month__quarter AS ds_month__quarter
        , subq_1.ds_month__year AS ds_month__year
        , subq_1.ds_month__extract_year AS ds_month__extract_year
        , subq_1.ds_month__extract_quarter AS ds_month__extract_quarter
        , subq_1.ds_month__extract_month AS ds_month__extract_month
        , subq_1.account__ds__day AS account__ds__day
        , subq_1.account__ds__week AS account__ds__week
        , subq_1.account__ds__month AS account__ds__month
        , subq_1.account__ds__quarter AS account__ds__quarter
        , subq_1.account__ds__year AS account__ds__year
        , subq_1.account__ds__extract_year AS account__ds__extract_year
        , subq_1.account__ds__extract_quarter AS account__ds__extract_quarter
        , subq_1.account__ds__extract_month AS account__ds__extract_month
        , subq_1.account__ds__extract_day AS account__ds__extract_day
        , subq_1.account__ds__extract_dow AS account__ds__extract_dow
        , subq_1.account__ds__extract_doy AS account__ds__extract_doy
        , subq_1.account__ds_month__month AS account__ds_month__month
        , subq_1.account__ds_month__quarter AS account__ds_month__quarter
        , subq_1.account__ds_month__year AS account__ds_month__year
        , subq_1.account__ds_month__extract_year AS account__ds_month__extract_year
        , subq_1.account__ds_month__extract_quarter AS account__ds_month__extract_quarter
        , subq_1.account__ds_month__extract_month AS account__ds_month__extract_month
        , subq_1.metric_time__month AS metric_time__month
        , subq_1.metric_time__quarter AS metric_time__quarter
        , subq_1.metric_time__year AS metric_time__year
        , subq_1.metric_time__extract_year AS metric_time__extract_year
        , subq_1.metric_time__extract_quarter AS metric_time__extract_quarter
        , subq_1.metric_time__extract_month AS metric_time__extract_month
        , subq_1.user AS user
        , subq_1.account__user AS account__user
        , subq_1.account_type AS account_type
        , subq_1.account__account_type AS account__account_type
        , subq_1.total_account_balance_first_day_of_month AS total_account_balance_first_day_of_month
      FROM (
        -- Metric Time Dimension 'ds_month'
        SELECT
          subq_0.ds__day
          , subq_0.ds__week
          , subq_0.ds__month
          , subq_0.ds__quarter
          , subq_0.ds__year
          , subq_0.ds__extract_year
          , subq_0.ds__extract_quarter
          , subq_0.ds__extract_month
          , subq_0.ds__extract_day
          , subq_0.ds__extract_dow
          , subq_0.ds__extract_doy
          , subq_0.ds_month__month
          , subq_0.ds_month__quarter
          , subq_0.ds_month__year
          , subq_0.ds_month__extract_year
          , subq_0.ds_month__extract_quarter
          , subq_0.ds_month__extract_month
          , subq_0.account__ds__day
          , subq_0.account__ds__week
          , subq_0.account__ds__month
          , subq_0.account__ds__quarter
          , subq_0.account__ds__year
          , subq_0.account__ds__extract_year
          , subq_0.account__ds__extract_quarter
          , subq_0.account__ds__extract_month
          , subq_0.account__ds__extract_day
          , subq_0.account__ds__extract_dow
          , subq_0.account__ds__extract_doy
          , subq_0.account__ds_month__month
          , subq_0.account__ds_month__quarter
          , subq_0.account__ds_month__year
          , subq_0.account__ds_month__extract_year
          , subq_0.account__ds_month__extract_quarter
          , subq_0.account__ds_month__extract_month
          , subq_0.ds_month__month AS metric_time__month
          , subq_0.ds_month__quarter AS metric_time__quarter
          , subq_0.ds_month__year AS metric_time__year
          , subq_0.ds_month__extract_year AS metric_time__extract_year
          , subq_0.ds_month__extract_quarter AS metric_time__extract_quarter
          , subq_0.ds_month__extract_month AS metric_time__extract_month
          , subq_0.user
          , subq_0.account__user
          , subq_0.account_type
          , subq_0.account__account_type
          , subq_0.total_account_balance_first_day_of_month
        FROM (
          -- Read Elements From Semantic Model 'accounts_source'
          SELECT
            accounts_source_src_28000.account_balance
            , accounts_source_src_28000.account_balance AS total_account_balance_first_day
            , accounts_source_src_28000.account_balance AS current_account_balance_by_user
            , accounts_source_src_28000.account_balance AS total_account_balance_first_day_of_month
            , DATETIME_TRUNC(accounts_source_src_28000.ds, day) AS ds__day
            , DATETIME_TRUNC(accounts_source_src_28000.ds, isoweek) AS ds__week
            , DATETIME_TRUNC(accounts_source_src_28000.ds, month) AS ds__month
            , DATETIME_TRUNC(accounts_source_src_28000.ds, quarter) AS ds__quarter
            , DATETIME_TRUNC(accounts_source_src_28000.ds, year) AS ds__year
            , EXTRACT(year FROM accounts_source_src_28000.ds) AS ds__extract_year
            , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS ds__extract_quarter
            , EXTRACT(month FROM accounts_source_src_28000.ds) AS ds__extract_month
            , EXTRACT(day FROM accounts_source_src_28000.ds) AS ds__extract_day
            , IF(EXTRACT(dayofweek FROM accounts_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM accounts_source_src_28000.ds) - 1) AS ds__extract_dow
            , EXTRACT(dayofyear FROM accounts_source_src_28000.ds) AS ds__extract_doy
            , DATETIME_TRUNC(accounts_source_src_28000.ds_month, month) AS ds_month__month
            , DATETIME_TRUNC(accounts_source_src_28000.ds_month, quarter) AS ds_month__quarter
            , DATETIME_TRUNC(accounts_source_src_28000.ds_month, year) AS ds_month__year
            , EXTRACT(year FROM accounts_source_src_28000.ds_month) AS ds_month__extract_year
            , EXTRACT(quarter FROM accounts_source_src_28000.ds_month) AS ds_month__extract_quarter
            , EXTRACT(month FROM accounts_source_src_28000.ds_month) AS ds_month__extract_month
            , accounts_source_src_28000.account_type
            , DATETIME_TRUNC(accounts_source_src_28000.ds, day) AS account__ds__day
            , DATETIME_TRUNC(accounts_source_src_28000.ds, isoweek) AS account__ds__week
            , DATETIME_TRUNC(accounts_source_src_28000.ds, month) AS account__ds__month
            , DATETIME_TRUNC(accounts_source_src_28000.ds, quarter) AS account__ds__quarter
            , DATETIME_TRUNC(accounts_source_src_28000.ds, year) AS account__ds__year
            , EXTRACT(year FROM accounts_source_src_28000.ds) AS account__ds__extract_year
            , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS account__ds__extract_quarter
            , EXTRACT(month FROM accounts_source_src_28000.ds) AS account__ds__extract_month
            , EXTRACT(day FROM accounts_source_src_28000.ds) AS account__ds__extract_day
            , IF(EXTRACT(dayofweek FROM accounts_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM accounts_source_src_28000.ds) - 1) AS account__ds__extract_dow
            , EXTRACT(dayofyear FROM accounts_source_src_28000.ds) AS account__ds__extract_doy
            , DATETIME_TRUNC(accounts_source_src_28000.ds_month, month) AS account__ds_month__month
            , DATETIME_TRUNC(accounts_source_src_28000.ds_month, quarter) AS account__ds_month__quarter
            , DATETIME_TRUNC(accounts_source_src_28000.ds_month, year) AS account__ds_month__year
            , EXTRACT(year FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_year
            , EXTRACT(quarter FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_quarter
            , EXTRACT(month FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_month
            , accounts_source_src_28000.account_type AS account__account_type
            , accounts_source_src_28000.user_id AS user
            , accounts_source_src_28000.user_id AS account__user
          FROM ***************************.fct_accounts accounts_source_src_28000
        ) subq_0
      ) subq_1
      INNER JOIN (
        -- Filter row on MIN(ds_month__month)
        SELECT
          MIN(subq_2.ds_month__month) AS ds_month__month__complete
        FROM (
          -- Metric Time Dimension 'ds_month'
          SELECT
            subq_0.ds__day
            , subq_0.ds__week
            , subq_0.ds__month
            , subq_0.ds__quarter
            , subq_0.ds__year
            , subq_0.ds__extract_year
            , subq_0.ds__extract_quarter
            , subq_0.ds__extract_month
            , subq_0.ds__extract_day
            , subq_0.ds__extract_dow
            , subq_0.ds__extract_doy
            , subq_0.ds_month__month
            , subq_0.ds_month__quarter
            , subq_0.ds_month__year
            , subq_0.ds_month__extract_year
            , subq_0.ds_month__extract_quarter
            , subq_0.ds_month__extract_month
            , subq_0.account__ds__day
            , subq_0.account__ds__week
            , subq_0.account__ds__month
            , subq_0.account__ds__quarter
            , subq_0.account__ds__year
            , subq_0.account__ds__extract_year
            , subq_0.account__ds__extract_quarter
            , subq_0.account__ds__extract_month
            , subq_0.account__ds__extract_day
            , subq_0.account__ds__extract_dow
            , subq_0.account__ds__extract_doy
            , subq_0.account__ds_month__month
            , subq_0.account__ds_month__quarter
            , subq_0.account__ds_month__year
            , subq_0.account__ds_month__extract_year
            , subq_0.account__ds_month__extract_quarter
            , subq_0.account__ds_month__extract_month
            , subq_0.ds_month__month AS metric_time__month
            , subq_0.ds_month__quarter AS metric_time__quarter
            , subq_0.ds_month__year AS metric_time__year
            , subq_0.ds_month__extract_year AS metric_time__extract_year
            , subq_0.ds_month__extract_quarter AS metric_time__extract_quarter
            , subq_0.ds_month__extract_month AS metric_time__extract_month
            , subq_0.user
            , subq_0.account__user
            , subq_0.account_type
            , subq_0.account__account_type
            , subq_0.total_account_balance_first_day_of_month
          FROM (
            -- Read Elements From Semantic Model 'accounts_source'
            SELECT
              accounts_source_src_28000.account_balance
              , accounts_source_src_28000.account_balance AS total_account_balance_first_day
              , accounts_source_src_28000.account_balance AS current_account_balance_by_user
              , accounts_source_src_28000.account_balance AS total_account_balance_first_day_of_month
              , DATETIME_TRUNC(accounts_source_src_28000.ds, day) AS ds__day
              , DATETIME_TRUNC(accounts_source_src_28000.ds, isoweek) AS ds__week
              , DATETIME_TRUNC(accounts_source_src_28000.ds, month) AS ds__month
              , DATETIME_TRUNC(accounts_source_src_28000.ds, quarter) AS ds__quarter
              , DATETIME_TRUNC(accounts_source_src_28000.ds, year) AS ds__year
              , EXTRACT(year FROM accounts_source_src_28000.ds) AS ds__extract_year
              , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS ds__extract_quarter
              , EXTRACT(month FROM accounts_source_src_28000.ds) AS ds__extract_month
              , EXTRACT(day FROM accounts_source_src_28000.ds) AS ds__extract_day
              , IF(EXTRACT(dayofweek FROM accounts_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM accounts_source_src_28000.ds) - 1) AS ds__extract_dow
              , EXTRACT(dayofyear FROM accounts_source_src_28000.ds) AS ds__extract_doy
              , DATETIME_TRUNC(accounts_source_src_28000.ds_month, month) AS ds_month__month
              , DATETIME_TRUNC(accounts_source_src_28000.ds_month, quarter) AS ds_month__quarter
              , DATETIME_TRUNC(accounts_source_src_28000.ds_month, year) AS ds_month__year
              , EXTRACT(year FROM accounts_source_src_28000.ds_month) AS ds_month__extract_year
              , EXTRACT(quarter FROM accounts_source_src_28000.ds_month) AS ds_month__extract_quarter
              , EXTRACT(month FROM accounts_source_src_28000.ds_month) AS ds_month__extract_month
              , accounts_source_src_28000.account_type
              , DATETIME_TRUNC(accounts_source_src_28000.ds, day) AS account__ds__day
              , DATETIME_TRUNC(accounts_source_src_28000.ds, isoweek) AS account__ds__week
              , DATETIME_TRUNC(accounts_source_src_28000.ds, month) AS account__ds__month
              , DATETIME_TRUNC(accounts_source_src_28000.ds, quarter) AS account__ds__quarter
              , DATETIME_TRUNC(accounts_source_src_28000.ds, year) AS account__ds__year
              , EXTRACT(year FROM accounts_source_src_28000.ds) AS account__ds__extract_year
              , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS account__ds__extract_quarter
              , EXTRACT(month FROM accounts_source_src_28000.ds) AS account__ds__extract_month
              , EXTRACT(day FROM accounts_source_src_28000.ds) AS account__ds__extract_day
              , IF(EXTRACT(dayofweek FROM accounts_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM accounts_source_src_28000.ds) - 1) AS account__ds__extract_dow
              , EXTRACT(dayofyear FROM accounts_source_src_28000.ds) AS account__ds__extract_doy
              , DATETIME_TRUNC(accounts_source_src_28000.ds_month, month) AS account__ds_month__month
              , DATETIME_TRUNC(accounts_source_src_28000.ds_month, quarter) AS account__ds_month__quarter
              , DATETIME_TRUNC(accounts_source_src_28000.ds_month, year) AS account__ds_month__year
              , EXTRACT(year FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_year
              , EXTRACT(quarter FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_quarter
              , EXTRACT(month FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_month
              , accounts_source_src_28000.account_type AS account__account_type
              , accounts_source_src_28000.user_id AS user
              , accounts_source_src_28000.user_id AS account__user
            FROM ***************************.fct_accounts accounts_source_src_28000
          ) subq_0
        ) subq_2
      ) subq_3
      ON
        subq_1.ds_month__month = subq_3.ds_month__month__complete
    ) subq_4
  ) subq_5
) subq_6
