test_name: test_cumulative_metric_with_metric_definition_filter
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a cumulative metric that has a filter defined in the YAML metric definition.
sql_engine: ClickHouse
---
SELECT
  subq_13.metric_time__day
  , subq_13.trailing_2_months_revenue_with_filter
FROM (
  SELECT
    subq_12.metric_time__day
    , subq_12.revenue AS trailing_2_months_revenue_with_filter
  FROM (
    SELECT
      subq_11.metric_time__day
      , subq_11.__revenue AS revenue
    FROM (
      SELECT
        subq_10.metric_time__day
        , SUM(subq_10.__revenue) AS __revenue
      FROM (
        SELECT
          subq_9.metric_time__day
          , subq_9.__revenue
        FROM (
          SELECT
            subq_8.revenue AS __revenue
            , subq_8.user__home_state_latest
            , subq_8.metric_time__day
          FROM (
            SELECT
              subq_7.metric_time__day
              , subq_7.user__home_state_latest
              , subq_7.__revenue AS revenue
            FROM (
              SELECT
                subq_6.home_state_latest AS user__home_state_latest
                , subq_4.ds__day AS ds__day
                , subq_4.ds__week AS ds__week
                , subq_4.ds__month AS ds__month
                , subq_4.ds__quarter AS ds__quarter
                , subq_4.ds__year AS ds__year
                , subq_4.ds__extract_year AS ds__extract_year
                , subq_4.ds__extract_quarter AS ds__extract_quarter
                , subq_4.ds__extract_month AS ds__extract_month
                , subq_4.ds__extract_day AS ds__extract_day
                , subq_4.ds__extract_dow AS ds__extract_dow
                , subq_4.ds__extract_doy AS ds__extract_doy
                , subq_4.revenue_instance__ds__day AS revenue_instance__ds__day
                , subq_4.revenue_instance__ds__week AS revenue_instance__ds__week
                , subq_4.revenue_instance__ds__month AS revenue_instance__ds__month
                , subq_4.revenue_instance__ds__quarter AS revenue_instance__ds__quarter
                , subq_4.revenue_instance__ds__year AS revenue_instance__ds__year
                , subq_4.revenue_instance__ds__extract_year AS revenue_instance__ds__extract_year
                , subq_4.revenue_instance__ds__extract_quarter AS revenue_instance__ds__extract_quarter
                , subq_4.revenue_instance__ds__extract_month AS revenue_instance__ds__extract_month
                , subq_4.revenue_instance__ds__extract_day AS revenue_instance__ds__extract_day
                , subq_4.revenue_instance__ds__extract_dow AS revenue_instance__ds__extract_dow
                , subq_4.revenue_instance__ds__extract_doy AS revenue_instance__ds__extract_doy
                , subq_4.metric_time__day AS metric_time__day
                , subq_4.metric_time__week AS metric_time__week
                , subq_4.metric_time__month AS metric_time__month
                , subq_4.metric_time__quarter AS metric_time__quarter
                , subq_4.metric_time__year AS metric_time__year
                , subq_4.metric_time__extract_year AS metric_time__extract_year
                , subq_4.metric_time__extract_quarter AS metric_time__extract_quarter
                , subq_4.metric_time__extract_month AS metric_time__extract_month
                , subq_4.metric_time__extract_day AS metric_time__extract_day
                , subq_4.metric_time__extract_dow AS metric_time__extract_dow
                , subq_4.metric_time__extract_doy AS metric_time__extract_doy
                , subq_4.user AS user
                , subq_4.revenue_instance__user AS revenue_instance__user
                , subq_4.__revenue AS __revenue
              FROM (
                SELECT
                  subq_2.metric_time__day AS metric_time__day
                  , subq_1.ds__day AS ds__day
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
                  , subq_1.revenue_instance__ds__day AS revenue_instance__ds__day
                  , subq_1.revenue_instance__ds__week AS revenue_instance__ds__week
                  , subq_1.revenue_instance__ds__month AS revenue_instance__ds__month
                  , subq_1.revenue_instance__ds__quarter AS revenue_instance__ds__quarter
                  , subq_1.revenue_instance__ds__year AS revenue_instance__ds__year
                  , subq_1.revenue_instance__ds__extract_year AS revenue_instance__ds__extract_year
                  , subq_1.revenue_instance__ds__extract_quarter AS revenue_instance__ds__extract_quarter
                  , subq_1.revenue_instance__ds__extract_month AS revenue_instance__ds__extract_month
                  , subq_1.revenue_instance__ds__extract_day AS revenue_instance__ds__extract_day
                  , subq_1.revenue_instance__ds__extract_dow AS revenue_instance__ds__extract_dow
                  , subq_1.revenue_instance__ds__extract_doy AS revenue_instance__ds__extract_doy
                  , subq_1.metric_time__week AS metric_time__week
                  , subq_1.metric_time__month AS metric_time__month
                  , subq_1.metric_time__quarter AS metric_time__quarter
                  , subq_1.metric_time__year AS metric_time__year
                  , subq_1.metric_time__extract_year AS metric_time__extract_year
                  , subq_1.metric_time__extract_quarter AS metric_time__extract_quarter
                  , subq_1.metric_time__extract_month AS metric_time__extract_month
                  , subq_1.metric_time__extract_day AS metric_time__extract_day
                  , subq_1.metric_time__extract_dow AS metric_time__extract_dow
                  , subq_1.metric_time__extract_doy AS metric_time__extract_doy
                  , subq_1.user AS user
                  , subq_1.revenue_instance__user AS revenue_instance__user
                  , subq_1.__revenue AS __revenue
                FROM (
                  SELECT
                    subq_3.ds AS metric_time__day
                  FROM ***************************.mf_time_spine subq_3
                ) subq_2
                INNER JOIN (
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
                    , subq_0.revenue_instance__ds__day
                    , subq_0.revenue_instance__ds__week
                    , subq_0.revenue_instance__ds__month
                    , subq_0.revenue_instance__ds__quarter
                    , subq_0.revenue_instance__ds__year
                    , subq_0.revenue_instance__ds__extract_year
                    , subq_0.revenue_instance__ds__extract_quarter
                    , subq_0.revenue_instance__ds__extract_month
                    , subq_0.revenue_instance__ds__extract_day
                    , subq_0.revenue_instance__ds__extract_dow
                    , subq_0.revenue_instance__ds__extract_doy
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
                    , subq_0.revenue_instance__user
                    , subq_0.__revenue
                  FROM (
                    SELECT
                      revenue_src_28000.revenue AS __revenue
                      , toStartOfDay(revenue_src_28000.created_at) AS ds__day
                      , toStartOfWeek(revenue_src_28000.created_at, 1) AS ds__week
                      , toStartOfMonth(revenue_src_28000.created_at) AS ds__month
                      , toStartOfQuarter(revenue_src_28000.created_at) AS ds__quarter
                      , toStartOfYear(revenue_src_28000.created_at) AS ds__year
                      , toYear(revenue_src_28000.created_at) AS ds__extract_year
                      , toQuarter(revenue_src_28000.created_at) AS ds__extract_quarter
                      , toMonth(revenue_src_28000.created_at) AS ds__extract_month
                      , toDayOfMonth(revenue_src_28000.created_at) AS ds__extract_day
                      , toDayOfWeek(revenue_src_28000.created_at) AS ds__extract_dow
                      , toDayOfYear(revenue_src_28000.created_at) AS ds__extract_doy
                      , toStartOfDay(revenue_src_28000.created_at) AS revenue_instance__ds__day
                      , toStartOfWeek(revenue_src_28000.created_at, 1) AS revenue_instance__ds__week
                      , toStartOfMonth(revenue_src_28000.created_at) AS revenue_instance__ds__month
                      , toStartOfQuarter(revenue_src_28000.created_at) AS revenue_instance__ds__quarter
                      , toStartOfYear(revenue_src_28000.created_at) AS revenue_instance__ds__year
                      , toYear(revenue_src_28000.created_at) AS revenue_instance__ds__extract_year
                      , toQuarter(revenue_src_28000.created_at) AS revenue_instance__ds__extract_quarter
                      , toMonth(revenue_src_28000.created_at) AS revenue_instance__ds__extract_month
                      , toDayOfMonth(revenue_src_28000.created_at) AS revenue_instance__ds__extract_day
                      , toDayOfWeek(revenue_src_28000.created_at) AS revenue_instance__ds__extract_dow
                      , toDayOfYear(revenue_src_28000.created_at) AS revenue_instance__ds__extract_doy
                      , revenue_src_28000.user_id AS user
                      , revenue_src_28000.user_id AS revenue_instance__user
                    FROM ***************************.fct_revenue revenue_src_28000
                  ) subq_0
                ) subq_1
                ON
                  (
                    subq_1.metric_time__day <= subq_2.metric_time__day
                  ) AND (
                    subq_1.metric_time__day > addMonths(subq_2.metric_time__day, -2)
                  )
              ) subq_4
              LEFT OUTER JOIN (
                SELECT
                  subq_5.user
                  , subq_5.home_state_latest
                FROM (
                  SELECT
                    toStartOfDay(users_latest_src_28000.ds) AS ds_latest__day
                    , toStartOfWeek(users_latest_src_28000.ds, 1) AS ds_latest__week
                    , toStartOfMonth(users_latest_src_28000.ds) AS ds_latest__month
                    , toStartOfQuarter(users_latest_src_28000.ds) AS ds_latest__quarter
                    , toStartOfYear(users_latest_src_28000.ds) AS ds_latest__year
                    , toYear(users_latest_src_28000.ds) AS ds_latest__extract_year
                    , toQuarter(users_latest_src_28000.ds) AS ds_latest__extract_quarter
                    , toMonth(users_latest_src_28000.ds) AS ds_latest__extract_month
                    , toDayOfMonth(users_latest_src_28000.ds) AS ds_latest__extract_day
                    , toDayOfWeek(users_latest_src_28000.ds) AS ds_latest__extract_dow
                    , toDayOfYear(users_latest_src_28000.ds) AS ds_latest__extract_doy
                    , users_latest_src_28000.home_state_latest
                    , toStartOfDay(users_latest_src_28000.ds) AS user__ds_latest__day
                    , toStartOfWeek(users_latest_src_28000.ds, 1) AS user__ds_latest__week
                    , toStartOfMonth(users_latest_src_28000.ds) AS user__ds_latest__month
                    , toStartOfQuarter(users_latest_src_28000.ds) AS user__ds_latest__quarter
                    , toStartOfYear(users_latest_src_28000.ds) AS user__ds_latest__year
                    , toYear(users_latest_src_28000.ds) AS user__ds_latest__extract_year
                    , toQuarter(users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
                    , toMonth(users_latest_src_28000.ds) AS user__ds_latest__extract_month
                    , toDayOfMonth(users_latest_src_28000.ds) AS user__ds_latest__extract_day
                    , toDayOfWeek(users_latest_src_28000.ds) AS user__ds_latest__extract_dow
                    , toDayOfYear(users_latest_src_28000.ds) AS user__ds_latest__extract_doy
                    , users_latest_src_28000.home_state_latest AS user__home_state_latest
                    , users_latest_src_28000.user_id AS user
                  FROM ***************************.dim_users_latest users_latest_src_28000
                ) subq_5
              ) subq_6
              ON
                subq_4.user = subq_6.user
            ) subq_7
          ) subq_8
          WHERE user__home_state_latest = 'CA'
        ) subq_9
      ) subq_10
      GROUP BY
        subq_10.metric_time__day
    ) subq_11
  ) subq_12
) subq_13
