test_name: test_non_additive_dimension_with_non_default_grain
test_filename: test_query_rendering.py
docstring:
  Tests querying a metric with a non-additive agg_time_dimension that has non-default granularity.
sql_engine: Snowflake
---
-- Write to DataTable
SELECT
  subq_8.total_account_balance_first_day_of_month
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_7.__total_account_balance_first_day_of_month AS total_account_balance_first_day_of_month
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      SUM(subq_6.__total_account_balance_first_day_of_month) AS __total_account_balance_first_day_of_month
    FROM (
      -- Pass Only Elements: ['__total_account_balance_first_day_of_month']
      SELECT
        subq_5.__total_account_balance_first_day_of_month
      FROM (
        -- Join on MIN(ds_month) and [] grouping by None
        SELECT
          subq_2.ds_month__month AS ds_month__month
          , subq_2.__total_account_balance_first_day_of_month AS __total_account_balance_first_day_of_month
        FROM (
          -- Pass Only Elements: ['__total_account_balance_first_day_of_month', 'ds_month__month']
          SELECT
            subq_1.ds_month__month
            , subq_1.__total_account_balance_first_day_of_month
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
              , subq_0.__total_account_balance_first_day_of_month
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
            ) subq_0
          ) subq_1
        ) subq_2
        INNER JOIN (
          -- Filter row on MIN(ds_month__month)
          SELECT
            MIN(subq_3.ds_month__month) AS ds_month__month__complete
          FROM (
            -- Pass Only Elements: ['__total_account_balance_first_day_of_month', 'ds_month__month']
            SELECT
              subq_1.ds_month__month
              , subq_1.__total_account_balance_first_day_of_month
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
                , subq_0.__total_account_balance_first_day_of_month
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
              ) subq_0
            ) subq_1
          ) subq_3
        ) subq_4
        ON
          subq_2.ds_month__month = subq_4.ds_month__month__complete
      ) subq_5
    ) subq_6
  ) subq_7
) subq_8
