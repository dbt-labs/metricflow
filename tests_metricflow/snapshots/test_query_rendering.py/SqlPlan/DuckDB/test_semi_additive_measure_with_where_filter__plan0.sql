test_name: test_semi_additive_measure_with_where_filter
test_filename: test_query_rendering.py
docstring:
  Tests querying a semi-additive measure with a where filter.
sql_engine: DuckDB
---
-- Write to DataTable
SELECT
  subq_8.user
  , subq_8.current_account_balance_by_user
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_7.user
    , subq_7.__current_account_balance_by_user AS current_account_balance_by_user
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_6.user
      , SUM(subq_6.__current_account_balance_by_user) AS __current_account_balance_by_user
    FROM (
      -- Pass Only Elements: ['__current_account_balance_by_user', 'user']
      SELECT
        subq_5.user
        , subq_5.__current_account_balance_by_user
      FROM (
        -- Join on MAX(ds) and ['user'] grouping by None
        SELECT
          subq_2.ds__day AS ds__day
          , subq_2.ds__week AS ds__week
          , subq_2.ds__month AS ds__month
          , subq_2.ds__quarter AS ds__quarter
          , subq_2.ds__year AS ds__year
          , subq_2.ds__extract_year AS ds__extract_year
          , subq_2.ds__extract_quarter AS ds__extract_quarter
          , subq_2.ds__extract_month AS ds__extract_month
          , subq_2.ds__extract_day AS ds__extract_day
          , subq_2.ds__extract_dow AS ds__extract_dow
          , subq_2.ds__extract_doy AS ds__extract_doy
          , subq_2.ds_month__month AS ds_month__month
          , subq_2.ds_month__quarter AS ds_month__quarter
          , subq_2.ds_month__year AS ds_month__year
          , subq_2.ds_month__extract_year AS ds_month__extract_year
          , subq_2.ds_month__extract_quarter AS ds_month__extract_quarter
          , subq_2.ds_month__extract_month AS ds_month__extract_month
          , subq_2.account__ds__day AS account__ds__day
          , subq_2.account__ds__week AS account__ds__week
          , subq_2.account__ds__month AS account__ds__month
          , subq_2.account__ds__quarter AS account__ds__quarter
          , subq_2.account__ds__year AS account__ds__year
          , subq_2.account__ds__extract_year AS account__ds__extract_year
          , subq_2.account__ds__extract_quarter AS account__ds__extract_quarter
          , subq_2.account__ds__extract_month AS account__ds__extract_month
          , subq_2.account__ds__extract_day AS account__ds__extract_day
          , subq_2.account__ds__extract_dow AS account__ds__extract_dow
          , subq_2.account__ds__extract_doy AS account__ds__extract_doy
          , subq_2.account__ds_month__month AS account__ds_month__month
          , subq_2.account__ds_month__quarter AS account__ds_month__quarter
          , subq_2.account__ds_month__year AS account__ds_month__year
          , subq_2.account__ds_month__extract_year AS account__ds_month__extract_year
          , subq_2.account__ds_month__extract_quarter AS account__ds_month__extract_quarter
          , subq_2.account__ds_month__extract_month AS account__ds_month__extract_month
          , subq_2.metric_time__day AS metric_time__day
          , subq_2.metric_time__week AS metric_time__week
          , subq_2.metric_time__month AS metric_time__month
          , subq_2.metric_time__quarter AS metric_time__quarter
          , subq_2.metric_time__year AS metric_time__year
          , subq_2.metric_time__extract_year AS metric_time__extract_year
          , subq_2.metric_time__extract_quarter AS metric_time__extract_quarter
          , subq_2.metric_time__extract_month AS metric_time__extract_month
          , subq_2.metric_time__extract_day AS metric_time__extract_day
          , subq_2.metric_time__extract_dow AS metric_time__extract_dow
          , subq_2.metric_time__extract_doy AS metric_time__extract_doy
          , subq_2.user AS user
          , subq_2.account__user AS account__user
          , subq_2.account_type AS account_type
          , subq_2.account__account_type AS account__account_type
          , subq_2.__account_balance AS __account_balance
          , subq_2.__total_account_balance_first_day AS __total_account_balance_first_day
          , subq_2.__current_account_balance_by_user AS __current_account_balance_by_user
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_1.account_balance AS __account_balance
            , subq_1.total_account_balance_first_day AS __total_account_balance_first_day
            , subq_1.current_account_balance_by_user AS __current_account_balance_by_user
            , subq_1.account_type
            , subq_1.account__account_type
            , subq_1.ds__day
            , subq_1.ds__week
            , subq_1.ds__month
            , subq_1.ds__quarter
            , subq_1.ds__year
            , subq_1.ds__extract_year
            , subq_1.ds__extract_quarter
            , subq_1.ds__extract_month
            , subq_1.ds__extract_day
            , subq_1.ds__extract_dow
            , subq_1.ds__extract_doy
            , subq_1.ds_month__month
            , subq_1.ds_month__quarter
            , subq_1.ds_month__year
            , subq_1.ds_month__extract_year
            , subq_1.ds_month__extract_quarter
            , subq_1.ds_month__extract_month
            , subq_1.account__ds__day
            , subq_1.account__ds__week
            , subq_1.account__ds__month
            , subq_1.account__ds__quarter
            , subq_1.account__ds__year
            , subq_1.account__ds__extract_year
            , subq_1.account__ds__extract_quarter
            , subq_1.account__ds__extract_month
            , subq_1.account__ds__extract_day
            , subq_1.account__ds__extract_dow
            , subq_1.account__ds__extract_doy
            , subq_1.account__ds_month__month
            , subq_1.account__ds_month__quarter
            , subq_1.account__ds_month__year
            , subq_1.account__ds_month__extract_year
            , subq_1.account__ds_month__extract_quarter
            , subq_1.account__ds_month__extract_month
            , subq_1.metric_time__day
            , subq_1.metric_time__week
            , subq_1.metric_time__month
            , subq_1.metric_time__quarter
            , subq_1.metric_time__year
            , subq_1.metric_time__extract_year
            , subq_1.metric_time__extract_quarter
            , subq_1.metric_time__extract_month
            , subq_1.metric_time__extract_day
            , subq_1.metric_time__extract_dow
            , subq_1.metric_time__extract_doy
            , subq_1.user
            , subq_1.account__user
          FROM (
            -- Metric Time Dimension 'ds'
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
              , subq_0.ds__day AS metric_time__day
              , subq_0.ds__week AS metric_time__week
              , subq_0.ds__month AS metric_time__month
              , subq_0.ds__quarter AS metric_time__quarter
              , subq_0.ds__year AS metric_time__year
              , subq_0.ds__extract_year AS metric_time__extract_year
              , subq_0.ds__extract_quarter AS metric_time__extract_quarter
              , subq_0.ds__extract_month AS metric_time__extract_month
              , subq_0.ds__extract_day AS metric_time__extract_day
              , subq_0.ds__extract_dow AS metric_time__extract_dow
              , subq_0.ds__extract_doy AS metric_time__extract_doy
              , subq_0.user
              , subq_0.account__user
              , subq_0.account_type
              , subq_0.account__account_type
              , subq_0.__account_balance AS account_balance
              , subq_0.__total_account_balance_first_day AS total_account_balance_first_day
              , subq_0.__current_account_balance_by_user AS current_account_balance_by_user
            FROM (
              -- Read Elements From Semantic Model 'accounts_source'
              SELECT
                accounts_source_src_28000.account_balance AS __account_balance
                , accounts_source_src_28000.account_balance AS __total_account_balance_first_day
                , accounts_source_src_28000.account_balance AS __current_account_balance_by_user
                , accounts_source_src_28000.account_balance AS __total_account_balance_first_day_of_month
                , DATE_TRUNC('day', accounts_source_src_28000.ds) AS ds__day
                , DATE_TRUNC('week', accounts_source_src_28000.ds) AS ds__week
                , DATE_TRUNC('month', accounts_source_src_28000.ds) AS ds__month
                , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS ds__quarter
                , DATE_TRUNC('year', accounts_source_src_28000.ds) AS ds__year
                , EXTRACT(year FROM accounts_source_src_28000.ds) AS ds__extract_year
                , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS ds__extract_quarter
                , EXTRACT(month FROM accounts_source_src_28000.ds) AS ds__extract_month
                , EXTRACT(day FROM accounts_source_src_28000.ds) AS ds__extract_day
                , EXTRACT(isodow FROM accounts_source_src_28000.ds) AS ds__extract_dow
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
                , EXTRACT(isodow FROM accounts_source_src_28000.ds) AS account__ds__extract_dow
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
            ) subq_0
          ) subq_1
          WHERE account__account_type = 'savings'
        ) subq_2
        INNER JOIN (
          -- Filter row on MAX(ds__day)
          SELECT
            subq_3.user
            , MAX(subq_3.ds__day) AS ds__day__complete
          FROM (
            -- Constrain Output with WHERE
            SELECT
              subq_1.account_balance AS __account_balance
              , subq_1.total_account_balance_first_day AS __total_account_balance_first_day
              , subq_1.current_account_balance_by_user AS __current_account_balance_by_user
              , subq_1.account_type
              , subq_1.account__account_type
              , subq_1.ds__day
              , subq_1.ds__week
              , subq_1.ds__month
              , subq_1.ds__quarter
              , subq_1.ds__year
              , subq_1.ds__extract_year
              , subq_1.ds__extract_quarter
              , subq_1.ds__extract_month
              , subq_1.ds__extract_day
              , subq_1.ds__extract_dow
              , subq_1.ds__extract_doy
              , subq_1.ds_month__month
              , subq_1.ds_month__quarter
              , subq_1.ds_month__year
              , subq_1.ds_month__extract_year
              , subq_1.ds_month__extract_quarter
              , subq_1.ds_month__extract_month
              , subq_1.account__ds__day
              , subq_1.account__ds__week
              , subq_1.account__ds__month
              , subq_1.account__ds__quarter
              , subq_1.account__ds__year
              , subq_1.account__ds__extract_year
              , subq_1.account__ds__extract_quarter
              , subq_1.account__ds__extract_month
              , subq_1.account__ds__extract_day
              , subq_1.account__ds__extract_dow
              , subq_1.account__ds__extract_doy
              , subq_1.account__ds_month__month
              , subq_1.account__ds_month__quarter
              , subq_1.account__ds_month__year
              , subq_1.account__ds_month__extract_year
              , subq_1.account__ds_month__extract_quarter
              , subq_1.account__ds_month__extract_month
              , subq_1.metric_time__day
              , subq_1.metric_time__week
              , subq_1.metric_time__month
              , subq_1.metric_time__quarter
              , subq_1.metric_time__year
              , subq_1.metric_time__extract_year
              , subq_1.metric_time__extract_quarter
              , subq_1.metric_time__extract_month
              , subq_1.metric_time__extract_day
              , subq_1.metric_time__extract_dow
              , subq_1.metric_time__extract_doy
              , subq_1.user
              , subq_1.account__user
            FROM (
              -- Metric Time Dimension 'ds'
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
                , subq_0.ds__day AS metric_time__day
                , subq_0.ds__week AS metric_time__week
                , subq_0.ds__month AS metric_time__month
                , subq_0.ds__quarter AS metric_time__quarter
                , subq_0.ds__year AS metric_time__year
                , subq_0.ds__extract_year AS metric_time__extract_year
                , subq_0.ds__extract_quarter AS metric_time__extract_quarter
                , subq_0.ds__extract_month AS metric_time__extract_month
                , subq_0.ds__extract_day AS metric_time__extract_day
                , subq_0.ds__extract_dow AS metric_time__extract_dow
                , subq_0.ds__extract_doy AS metric_time__extract_doy
                , subq_0.user
                , subq_0.account__user
                , subq_0.account_type
                , subq_0.account__account_type
                , subq_0.__account_balance AS account_balance
                , subq_0.__total_account_balance_first_day AS total_account_balance_first_day
                , subq_0.__current_account_balance_by_user AS current_account_balance_by_user
              FROM (
                -- Read Elements From Semantic Model 'accounts_source'
                SELECT
                  accounts_source_src_28000.account_balance AS __account_balance
                  , accounts_source_src_28000.account_balance AS __total_account_balance_first_day
                  , accounts_source_src_28000.account_balance AS __current_account_balance_by_user
                  , accounts_source_src_28000.account_balance AS __total_account_balance_first_day_of_month
                  , DATE_TRUNC('day', accounts_source_src_28000.ds) AS ds__day
                  , DATE_TRUNC('week', accounts_source_src_28000.ds) AS ds__week
                  , DATE_TRUNC('month', accounts_source_src_28000.ds) AS ds__month
                  , DATE_TRUNC('quarter', accounts_source_src_28000.ds) AS ds__quarter
                  , DATE_TRUNC('year', accounts_source_src_28000.ds) AS ds__year
                  , EXTRACT(year FROM accounts_source_src_28000.ds) AS ds__extract_year
                  , EXTRACT(quarter FROM accounts_source_src_28000.ds) AS ds__extract_quarter
                  , EXTRACT(month FROM accounts_source_src_28000.ds) AS ds__extract_month
                  , EXTRACT(day FROM accounts_source_src_28000.ds) AS ds__extract_day
                  , EXTRACT(isodow FROM accounts_source_src_28000.ds) AS ds__extract_dow
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
                  , EXTRACT(isodow FROM accounts_source_src_28000.ds) AS account__ds__extract_dow
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
              ) subq_0
            ) subq_1
            WHERE account__account_type = 'savings'
          ) subq_3
          GROUP BY
            subq_3.user
        ) subq_4
        ON
          (
            subq_2.ds__day = subq_4.ds__day__complete
          ) AND (
            subq_2.user = subq_4.user
          )
      ) subq_5
    ) subq_6
    GROUP BY
      subq_6.user
  ) subq_7
) subq_8
