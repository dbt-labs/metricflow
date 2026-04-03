test_name: test_semi_additive_measure_with_where_filter
test_filename: test_query_rendering.py
docstring:
  Tests querying a semi-additive measure with a where filter.
sql_engine: ClickHouse
---
SELECT
  subq_9.user
  , subq_9.current_account_balance_by_user
FROM (
  SELECT
    subq_8.user
    , subq_8.__current_account_balance_by_user AS current_account_balance_by_user
  FROM (
    SELECT
      subq_7.user
      , SUM(subq_7.__current_account_balance_by_user) AS __current_account_balance_by_user
    FROM (
      SELECT
        subq_6.user
        , subq_6.__current_account_balance_by_user
      FROM (
        SELECT
          subq_3.ds__day AS ds__day
          , subq_3.user AS user
          , subq_3.account__account_type AS account__account_type
          , subq_3.__current_account_balance_by_user AS __current_account_balance_by_user
        FROM (
          SELECT
            subq_2.current_account_balance_by_user AS __current_account_balance_by_user
            , subq_2.account__account_type
            , subq_2.ds__day
            , subq_2.user
          FROM (
            SELECT
              subq_1.ds__day
              , subq_1.user
              , subq_1.account__account_type
              , subq_1.__current_account_balance_by_user AS current_account_balance_by_user
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
                , subq_0.__account_balance
                , subq_0.__total_account_balance_first_day
                , subq_0.__current_account_balance_by_user
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
          WHERE account__account_type = 'savings'
        ) subq_3
        INNER JOIN (
          SELECT
            subq_4.user
            , MAX(subq_4.ds__day) AS ds__day__complete
          FROM (
            SELECT
              subq_2.current_account_balance_by_user AS __current_account_balance_by_user
              , subq_2.account__account_type
              , subq_2.ds__day
              , subq_2.user
            FROM (
              SELECT
                subq_1.ds__day
                , subq_1.user
                , subq_1.account__account_type
                , subq_1.__current_account_balance_by_user AS current_account_balance_by_user
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
                  , subq_0.__account_balance
                  , subq_0.__total_account_balance_first_day
                  , subq_0.__current_account_balance_by_user
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
            WHERE account__account_type = 'savings'
          ) subq_4
          GROUP BY
            subq_4.user
        ) subq_5
        ON
          (
            subq_3.ds__day = subq_5.ds__day__complete
          ) AND (
            subq_3.user = subq_5.user
          )
      ) subq_6
    ) subq_7
    GROUP BY
      subq_7.user
  ) subq_8
) subq_9
