test_name: test_non_additive_dimension_with_non_default_grain
test_filename: test_query_rendering.py
docstring:
  Tests querying a metric with a non-additive agg_time_dimension that has non-default granularity.
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_5.total_account_balance_first_day_of_month
FROM (
  -- Aggregate Measures
  SELECT
    SUM(nr_subq_4.total_account_balance_first_day_of_month) AS total_account_balance_first_day_of_month
  FROM (
    -- Pass Only Elements: ['total_account_balance_first_day_of_month',]
    SELECT
      nr_subq_3.total_account_balance_first_day_of_month
    FROM (
      -- Join on MIN(ds_month) and [] grouping by None
      SELECT
        nr_subq_0.ds__day AS ds__day
        , nr_subq_0.ds__week AS ds__week
        , nr_subq_0.ds__month AS ds__month
        , nr_subq_0.ds__quarter AS ds__quarter
        , nr_subq_0.ds__year AS ds__year
        , nr_subq_0.ds__extract_year AS ds__extract_year
        , nr_subq_0.ds__extract_quarter AS ds__extract_quarter
        , nr_subq_0.ds__extract_month AS ds__extract_month
        , nr_subq_0.ds__extract_day AS ds__extract_day
        , nr_subq_0.ds__extract_dow AS ds__extract_dow
        , nr_subq_0.ds__extract_doy AS ds__extract_doy
        , nr_subq_0.ds_month__month AS ds_month__month
        , nr_subq_0.ds_month__quarter AS ds_month__quarter
        , nr_subq_0.ds_month__year AS ds_month__year
        , nr_subq_0.ds_month__extract_year AS ds_month__extract_year
        , nr_subq_0.ds_month__extract_quarter AS ds_month__extract_quarter
        , nr_subq_0.ds_month__extract_month AS ds_month__extract_month
        , nr_subq_0.account__ds__day AS account__ds__day
        , nr_subq_0.account__ds__week AS account__ds__week
        , nr_subq_0.account__ds__month AS account__ds__month
        , nr_subq_0.account__ds__quarter AS account__ds__quarter
        , nr_subq_0.account__ds__year AS account__ds__year
        , nr_subq_0.account__ds__extract_year AS account__ds__extract_year
        , nr_subq_0.account__ds__extract_quarter AS account__ds__extract_quarter
        , nr_subq_0.account__ds__extract_month AS account__ds__extract_month
        , nr_subq_0.account__ds__extract_day AS account__ds__extract_day
        , nr_subq_0.account__ds__extract_dow AS account__ds__extract_dow
        , nr_subq_0.account__ds__extract_doy AS account__ds__extract_doy
        , nr_subq_0.account__ds_month__month AS account__ds_month__month
        , nr_subq_0.account__ds_month__quarter AS account__ds_month__quarter
        , nr_subq_0.account__ds_month__year AS account__ds_month__year
        , nr_subq_0.account__ds_month__extract_year AS account__ds_month__extract_year
        , nr_subq_0.account__ds_month__extract_quarter AS account__ds_month__extract_quarter
        , nr_subq_0.account__ds_month__extract_month AS account__ds_month__extract_month
        , nr_subq_0.metric_time__month AS metric_time__month
        , nr_subq_0.metric_time__quarter AS metric_time__quarter
        , nr_subq_0.metric_time__year AS metric_time__year
        , nr_subq_0.metric_time__extract_year AS metric_time__extract_year
        , nr_subq_0.metric_time__extract_quarter AS metric_time__extract_quarter
        , nr_subq_0.metric_time__extract_month AS metric_time__extract_month
        , nr_subq_0.user AS user
        , nr_subq_0.account__user AS account__user
        , nr_subq_0.account_type AS account_type
        , nr_subq_0.account__account_type AS account__account_type
        , nr_subq_0.total_account_balance_first_day_of_month AS total_account_balance_first_day_of_month
      FROM (
        -- Metric Time Dimension 'ds_month'
        SELECT
          nr_subq_28001.ds__day
          , nr_subq_28001.ds__week
          , nr_subq_28001.ds__month
          , nr_subq_28001.ds__quarter
          , nr_subq_28001.ds__year
          , nr_subq_28001.ds__extract_year
          , nr_subq_28001.ds__extract_quarter
          , nr_subq_28001.ds__extract_month
          , nr_subq_28001.ds__extract_day
          , nr_subq_28001.ds__extract_dow
          , nr_subq_28001.ds__extract_doy
          , nr_subq_28001.ds_month__month
          , nr_subq_28001.ds_month__quarter
          , nr_subq_28001.ds_month__year
          , nr_subq_28001.ds_month__extract_year
          , nr_subq_28001.ds_month__extract_quarter
          , nr_subq_28001.ds_month__extract_month
          , nr_subq_28001.account__ds__day
          , nr_subq_28001.account__ds__week
          , nr_subq_28001.account__ds__month
          , nr_subq_28001.account__ds__quarter
          , nr_subq_28001.account__ds__year
          , nr_subq_28001.account__ds__extract_year
          , nr_subq_28001.account__ds__extract_quarter
          , nr_subq_28001.account__ds__extract_month
          , nr_subq_28001.account__ds__extract_day
          , nr_subq_28001.account__ds__extract_dow
          , nr_subq_28001.account__ds__extract_doy
          , nr_subq_28001.account__ds_month__month
          , nr_subq_28001.account__ds_month__quarter
          , nr_subq_28001.account__ds_month__year
          , nr_subq_28001.account__ds_month__extract_year
          , nr_subq_28001.account__ds_month__extract_quarter
          , nr_subq_28001.account__ds_month__extract_month
          , nr_subq_28001.ds_month__month AS metric_time__month
          , nr_subq_28001.ds_month__quarter AS metric_time__quarter
          , nr_subq_28001.ds_month__year AS metric_time__year
          , nr_subq_28001.ds_month__extract_year AS metric_time__extract_year
          , nr_subq_28001.ds_month__extract_quarter AS metric_time__extract_quarter
          , nr_subq_28001.ds_month__extract_month AS metric_time__extract_month
          , nr_subq_28001.user
          , nr_subq_28001.account__user
          , nr_subq_28001.account_type
          , nr_subq_28001.account__account_type
          , nr_subq_28001.total_account_balance_first_day_of_month
        FROM (
          -- Read Elements From Semantic Model 'accounts_source'
          SELECT
            accounts_source_src_28000.account_balance
            , accounts_source_src_28000.account_balance AS total_account_balance_first_day
            , accounts_source_src_28000.account_balance AS current_account_balance_by_user
            , accounts_source_src_28000.account_balance AS total_account_balance_first_day_of_month
            , DATE_TRUNC('day', accounts_source_src_28000.ds) AS ds__day
            , DATE_TRUNC('week', accounts_source_src_28000.ds) AS ds__week
            , DATE_TRUNC('month', accounts_source_src_28000.ds) AS ds__month
            , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS ds__quarter
            , DATE_TRUNC('year', accounts_source_src_28000.ds) AS ds__year
            , EXTRACT(year FROM accounts_source_src_28000.ds) AS ds__extract_year
            , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS ds__extract_quarter
            , EXTRACT(month FROM accounts_source_src_28000.ds) AS ds__extract_month
            , EXTRACT(day FROM accounts_source_src_28000.ds) AS ds__extract_day
            , EXTRACT(dayofweekiso FROM accounts_source_src_28000.ds) AS ds__extract_dow
            , EXTRACT(doy FROM accounts_source_src_28000.ds) AS ds__extract_doy
            , DATE_TRUNC('month', accounts_source_src_28000.ds_month) AS ds_month__month
            , DATE_TRUNC('quarter', accounts_source_src_28000.ds_month) AS ds_month__quarter
            , DATE_TRUNC('year', accounts_source_src_28000.ds_month) AS ds_month__year
            , EXTRACT(year FROM accounts_source_src_28000.ds_month) AS ds_month__extract_year
            , EXTRACT(quarter FROM accounts_source_src_28000.ds_month) AS ds_month__extract_quarter
            , EXTRACT(month FROM accounts_source_src_28000.ds_month) AS ds_month__extract_month
            , accounts_source_src_28000.account_type
            , DATE_TRUNC('day', accounts_source_src_28000.ds) AS account__ds__day
            , DATE_TRUNC('week', accounts_source_src_28000.ds) AS account__ds__week
            , DATE_TRUNC('month', accounts_source_src_28000.ds) AS account__ds__month
            , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS account__ds__quarter
            , DATE_TRUNC('year', accounts_source_src_28000.ds) AS account__ds__year
            , EXTRACT(year FROM accounts_source_src_28000.ds) AS account__ds__extract_year
            , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS account__ds__extract_quarter
            , EXTRACT(month FROM accounts_source_src_28000.ds) AS account__ds__extract_month
            , EXTRACT(day FROM accounts_source_src_28000.ds) AS account__ds__extract_day
            , EXTRACT(dayofweekiso FROM accounts_source_src_28000.ds) AS account__ds__extract_dow
            , EXTRACT(doy FROM accounts_source_src_28000.ds) AS account__ds__extract_doy
            , DATE_TRUNC('month', accounts_source_src_28000.ds_month) AS account__ds_month__month
            , DATE_TRUNC('quarter', accounts_source_src_28000.ds_month) AS account__ds_month__quarter
            , DATE_TRUNC('year', accounts_source_src_28000.ds_month) AS account__ds_month__year
            , EXTRACT(year FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_year
            , EXTRACT(quarter FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_quarter
            , EXTRACT(month FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_month
            , accounts_source_src_28000.account_type AS account__account_type
            , accounts_source_src_28000.user_id AS user
            , accounts_source_src_28000.user_id AS account__user
          FROM ***************************.fct_accounts accounts_source_src_28000
        ) nr_subq_28001
      ) nr_subq_0
      INNER JOIN (
        -- Filter row on MIN(ds_month__month)
        SELECT
          MIN(nr_subq_1.ds_month__month) AS ds_month__month__complete
        FROM (
          -- Metric Time Dimension 'ds_month'
          SELECT
            nr_subq_28001.ds__day
            , nr_subq_28001.ds__week
            , nr_subq_28001.ds__month
            , nr_subq_28001.ds__quarter
            , nr_subq_28001.ds__year
            , nr_subq_28001.ds__extract_year
            , nr_subq_28001.ds__extract_quarter
            , nr_subq_28001.ds__extract_month
            , nr_subq_28001.ds__extract_day
            , nr_subq_28001.ds__extract_dow
            , nr_subq_28001.ds__extract_doy
            , nr_subq_28001.ds_month__month
            , nr_subq_28001.ds_month__quarter
            , nr_subq_28001.ds_month__year
            , nr_subq_28001.ds_month__extract_year
            , nr_subq_28001.ds_month__extract_quarter
            , nr_subq_28001.ds_month__extract_month
            , nr_subq_28001.account__ds__day
            , nr_subq_28001.account__ds__week
            , nr_subq_28001.account__ds__month
            , nr_subq_28001.account__ds__quarter
            , nr_subq_28001.account__ds__year
            , nr_subq_28001.account__ds__extract_year
            , nr_subq_28001.account__ds__extract_quarter
            , nr_subq_28001.account__ds__extract_month
            , nr_subq_28001.account__ds__extract_day
            , nr_subq_28001.account__ds__extract_dow
            , nr_subq_28001.account__ds__extract_doy
            , nr_subq_28001.account__ds_month__month
            , nr_subq_28001.account__ds_month__quarter
            , nr_subq_28001.account__ds_month__year
            , nr_subq_28001.account__ds_month__extract_year
            , nr_subq_28001.account__ds_month__extract_quarter
            , nr_subq_28001.account__ds_month__extract_month
            , nr_subq_28001.ds_month__month AS metric_time__month
            , nr_subq_28001.ds_month__quarter AS metric_time__quarter
            , nr_subq_28001.ds_month__year AS metric_time__year
            , nr_subq_28001.ds_month__extract_year AS metric_time__extract_year
            , nr_subq_28001.ds_month__extract_quarter AS metric_time__extract_quarter
            , nr_subq_28001.ds_month__extract_month AS metric_time__extract_month
            , nr_subq_28001.user
            , nr_subq_28001.account__user
            , nr_subq_28001.account_type
            , nr_subq_28001.account__account_type
            , nr_subq_28001.total_account_balance_first_day_of_month
          FROM (
            -- Read Elements From Semantic Model 'accounts_source'
            SELECT
              accounts_source_src_28000.account_balance
              , accounts_source_src_28000.account_balance AS total_account_balance_first_day
              , accounts_source_src_28000.account_balance AS current_account_balance_by_user
              , accounts_source_src_28000.account_balance AS total_account_balance_first_day_of_month
              , DATE_TRUNC('day', accounts_source_src_28000.ds) AS ds__day
              , DATE_TRUNC('week', accounts_source_src_28000.ds) AS ds__week
              , DATE_TRUNC('month', accounts_source_src_28000.ds) AS ds__month
              , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS ds__quarter
              , DATE_TRUNC('year', accounts_source_src_28000.ds) AS ds__year
              , EXTRACT(year FROM accounts_source_src_28000.ds) AS ds__extract_year
              , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS ds__extract_quarter
              , EXTRACT(month FROM accounts_source_src_28000.ds) AS ds__extract_month
              , EXTRACT(day FROM accounts_source_src_28000.ds) AS ds__extract_day
              , EXTRACT(dayofweekiso FROM accounts_source_src_28000.ds) AS ds__extract_dow
              , EXTRACT(doy FROM accounts_source_src_28000.ds) AS ds__extract_doy
              , DATE_TRUNC('month', accounts_source_src_28000.ds_month) AS ds_month__month
              , DATE_TRUNC('quarter', accounts_source_src_28000.ds_month) AS ds_month__quarter
              , DATE_TRUNC('year', accounts_source_src_28000.ds_month) AS ds_month__year
              , EXTRACT(year FROM accounts_source_src_28000.ds_month) AS ds_month__extract_year
              , EXTRACT(quarter FROM accounts_source_src_28000.ds_month) AS ds_month__extract_quarter
              , EXTRACT(month FROM accounts_source_src_28000.ds_month) AS ds_month__extract_month
              , accounts_source_src_28000.account_type
              , DATE_TRUNC('day', accounts_source_src_28000.ds) AS account__ds__day
              , DATE_TRUNC('week', accounts_source_src_28000.ds) AS account__ds__week
              , DATE_TRUNC('month', accounts_source_src_28000.ds) AS account__ds__month
              , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS account__ds__quarter
              , DATE_TRUNC('year', accounts_source_src_28000.ds) AS account__ds__year
              , EXTRACT(year FROM accounts_source_src_28000.ds) AS account__ds__extract_year
              , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS account__ds__extract_quarter
              , EXTRACT(month FROM accounts_source_src_28000.ds) AS account__ds__extract_month
              , EXTRACT(day FROM accounts_source_src_28000.ds) AS account__ds__extract_day
              , EXTRACT(dayofweekiso FROM accounts_source_src_28000.ds) AS account__ds__extract_dow
              , EXTRACT(doy FROM accounts_source_src_28000.ds) AS account__ds__extract_doy
              , DATE_TRUNC('month', accounts_source_src_28000.ds_month) AS account__ds_month__month
              , DATE_TRUNC('quarter', accounts_source_src_28000.ds_month) AS account__ds_month__quarter
              , DATE_TRUNC('year', accounts_source_src_28000.ds_month) AS account__ds_month__year
              , EXTRACT(year FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_year
              , EXTRACT(quarter FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_quarter
              , EXTRACT(month FROM accounts_source_src_28000.ds_month) AS account__ds_month__extract_month
              , accounts_source_src_28000.account_type AS account__account_type
              , accounts_source_src_28000.user_id AS user
              , accounts_source_src_28000.user_id AS account__user
            FROM ***************************.fct_accounts accounts_source_src_28000
          ) nr_subq_28001
        ) nr_subq_1
      ) nr_subq_2
      ON
        nr_subq_0.ds_month__month = nr_subq_2.ds_month__month__complete
    ) nr_subq_3
  ) nr_subq_4
) nr_subq_5
