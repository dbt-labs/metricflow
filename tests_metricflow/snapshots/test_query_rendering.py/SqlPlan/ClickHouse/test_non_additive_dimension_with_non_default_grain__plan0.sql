test_name: test_non_additive_dimension_with_non_default_grain
test_filename: test_query_rendering.py
docstring:
  Tests querying a metric with a non-additive agg_time_dimension that has non-default granularity.
sql_engine: ClickHouse
---
SELECT
  subq_8.total_account_balance_first_day_of_month
FROM (
  SELECT
    subq_7.__total_account_balance_first_day_of_month AS total_account_balance_first_day_of_month
  FROM (
    SELECT
      SUM(subq_6.__total_account_balance_first_day_of_month) AS __total_account_balance_first_day_of_month
    FROM (
      SELECT
        subq_5.__total_account_balance_first_day_of_month
      FROM (
        SELECT
          subq_2.ds_month__month AS ds_month__month
          , subq_2.__total_account_balance_first_day_of_month AS __total_account_balance_first_day_of_month
        FROM (
          SELECT
            subq_1.ds_month__month
            , subq_1.__total_account_balance_first_day_of_month
          FROM (
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
              SELECT
                accounts_source_src_28000.account_balance AS __account_balance
                , accounts_source_src_28000.account_balance AS __total_account_balance_first_day
                , accounts_source_src_28000.account_balance AS __current_account_balance_by_user
                , accounts_source_src_28000.account_balance AS __total_account_balance_first_day_of_month
                , toStartOfDay(accounts_source_src_28000.ds) AS ds__day
                , toStartOfWeek(accounts_source_src_28000.ds, 1) AS ds__week
                , toStartOfMonth(accounts_source_src_28000.ds) AS ds__month
                , toStartOfQuarter(accounts_source_src_28000.ds) AS ds__quarter
                , toStartOfYear(accounts_source_src_28000.ds) AS ds__year
                , toYear(accounts_source_src_28000.ds) AS ds__extract_year
                , toQuarter(accounts_source_src_28000.ds) AS ds__extract_quarter
                , toMonth(accounts_source_src_28000.ds) AS ds__extract_month
                , toDayOfMonth(accounts_source_src_28000.ds) AS ds__extract_day
                , toDayOfWeek(accounts_source_src_28000.ds) AS ds__extract_dow
                , toDayOfYear(accounts_source_src_28000.ds) AS ds__extract_doy
                , toStartOfMonth(accounts_source_src_28000.ds_month) AS ds_month__month
                , toStartOfQuarter(accounts_source_src_28000.ds_month) AS ds_month__quarter
                , toStartOfYear(accounts_source_src_28000.ds_month) AS ds_month__year
                , toYear(accounts_source_src_28000.ds_month) AS ds_month__extract_year
                , toQuarter(accounts_source_src_28000.ds_month) AS ds_month__extract_quarter
                , toMonth(accounts_source_src_28000.ds_month) AS ds_month__extract_month
                , accounts_source_src_28000.account_type
                , toStartOfDay(accounts_source_src_28000.ds) AS account__ds__day
                , toStartOfWeek(accounts_source_src_28000.ds, 1) AS account__ds__week
                , toStartOfMonth(accounts_source_src_28000.ds) AS account__ds__month
                , toStartOfQuarter(accounts_source_src_28000.ds) AS account__ds__quarter
                , toStartOfYear(accounts_source_src_28000.ds) AS account__ds__year
                , toYear(accounts_source_src_28000.ds) AS account__ds__extract_year
                , toQuarter(accounts_source_src_28000.ds) AS account__ds__extract_quarter
                , toMonth(accounts_source_src_28000.ds) AS account__ds__extract_month
                , toDayOfMonth(accounts_source_src_28000.ds) AS account__ds__extract_day
                , toDayOfWeek(accounts_source_src_28000.ds) AS account__ds__extract_dow
                , toDayOfYear(accounts_source_src_28000.ds) AS account__ds__extract_doy
                , toStartOfMonth(accounts_source_src_28000.ds_month) AS account__ds_month__month
                , toStartOfQuarter(accounts_source_src_28000.ds_month) AS account__ds_month__quarter
                , toStartOfYear(accounts_source_src_28000.ds_month) AS account__ds_month__year
                , toYear(accounts_source_src_28000.ds_month) AS account__ds_month__extract_year
                , toQuarter(accounts_source_src_28000.ds_month) AS account__ds_month__extract_quarter
                , toMonth(accounts_source_src_28000.ds_month) AS account__ds_month__extract_month
                , accounts_source_src_28000.account_type AS account__account_type
                , accounts_source_src_28000.user_id AS user
                , accounts_source_src_28000.user_id AS account__user
              FROM ***************************.fct_accounts accounts_source_src_28000
            ) subq_0
          ) subq_1
        ) subq_2
        INNER JOIN (
          SELECT
            MIN(subq_3.ds_month__month) AS ds_month__month__complete
          FROM (
            SELECT
              subq_1.ds_month__month
              , subq_1.__total_account_balance_first_day_of_month
            FROM (
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
                SELECT
                  accounts_source_src_28000.account_balance AS __account_balance
                  , accounts_source_src_28000.account_balance AS __total_account_balance_first_day
                  , accounts_source_src_28000.account_balance AS __current_account_balance_by_user
                  , accounts_source_src_28000.account_balance AS __total_account_balance_first_day_of_month
                  , toStartOfDay(accounts_source_src_28000.ds) AS ds__day
                  , toStartOfWeek(accounts_source_src_28000.ds, 1) AS ds__week
                  , toStartOfMonth(accounts_source_src_28000.ds) AS ds__month
                  , toStartOfQuarter(accounts_source_src_28000.ds) AS ds__quarter
                  , toStartOfYear(accounts_source_src_28000.ds) AS ds__year
                  , toYear(accounts_source_src_28000.ds) AS ds__extract_year
                  , toQuarter(accounts_source_src_28000.ds) AS ds__extract_quarter
                  , toMonth(accounts_source_src_28000.ds) AS ds__extract_month
                  , toDayOfMonth(accounts_source_src_28000.ds) AS ds__extract_day
                  , toDayOfWeek(accounts_source_src_28000.ds) AS ds__extract_dow
                  , toDayOfYear(accounts_source_src_28000.ds) AS ds__extract_doy
                  , toStartOfMonth(accounts_source_src_28000.ds_month) AS ds_month__month
                  , toStartOfQuarter(accounts_source_src_28000.ds_month) AS ds_month__quarter
                  , toStartOfYear(accounts_source_src_28000.ds_month) AS ds_month__year
                  , toYear(accounts_source_src_28000.ds_month) AS ds_month__extract_year
                  , toQuarter(accounts_source_src_28000.ds_month) AS ds_month__extract_quarter
                  , toMonth(accounts_source_src_28000.ds_month) AS ds_month__extract_month
                  , accounts_source_src_28000.account_type
                  , toStartOfDay(accounts_source_src_28000.ds) AS account__ds__day
                  , toStartOfWeek(accounts_source_src_28000.ds, 1) AS account__ds__week
                  , toStartOfMonth(accounts_source_src_28000.ds) AS account__ds__month
                  , toStartOfQuarter(accounts_source_src_28000.ds) AS account__ds__quarter
                  , toStartOfYear(accounts_source_src_28000.ds) AS account__ds__year
                  , toYear(accounts_source_src_28000.ds) AS account__ds__extract_year
                  , toQuarter(accounts_source_src_28000.ds) AS account__ds__extract_quarter
                  , toMonth(accounts_source_src_28000.ds) AS account__ds__extract_month
                  , toDayOfMonth(accounts_source_src_28000.ds) AS account__ds__extract_day
                  , toDayOfWeek(accounts_source_src_28000.ds) AS account__ds__extract_dow
                  , toDayOfYear(accounts_source_src_28000.ds) AS account__ds__extract_doy
                  , toStartOfMonth(accounts_source_src_28000.ds_month) AS account__ds_month__month
                  , toStartOfQuarter(accounts_source_src_28000.ds_month) AS account__ds_month__quarter
                  , toStartOfYear(accounts_source_src_28000.ds_month) AS account__ds_month__year
                  , toYear(accounts_source_src_28000.ds_month) AS account__ds_month__extract_year
                  , toQuarter(accounts_source_src_28000.ds_month) AS account__ds_month__extract_quarter
                  , toMonth(accounts_source_src_28000.ds_month) AS account__ds_month__extract_month
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
