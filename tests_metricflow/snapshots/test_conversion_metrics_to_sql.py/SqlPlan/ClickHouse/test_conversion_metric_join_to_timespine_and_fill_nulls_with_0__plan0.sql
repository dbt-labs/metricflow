test_name: test_conversion_metric_join_to_timespine_and_fill_nulls_with_0
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric that joins to time spine and fills nulls with 0.
sql_engine: ClickHouse
---
SELECT
  subq_27.metric_time__day
  , subq_27.visit_buy_conversion_rate_7days_fill_nulls_with_0
FROM (
  SELECT
    subq_26.metric_time__day
    , CAST(subq_26.__buys_fill_nulls_with_0_join_to_timespine AS Nullable(Float64)) / CAST(NULLIF(subq_26.__visits_fill_nulls_with_0_join_to_timespine, 0) AS Nullable(Float64)) AS visit_buy_conversion_rate_7days_fill_nulls_with_0
  FROM (
    SELECT
      COALESCE(subq_9.metric_time__day, subq_25.metric_time__day) AS metric_time__day
      , COALESCE(MAX(subq_9.__visits_fill_nulls_with_0_join_to_timespine), 0) AS __visits_fill_nulls_with_0_join_to_timespine
      , COALESCE(MAX(subq_25.__buys_fill_nulls_with_0_join_to_timespine), 0) AS __buys_fill_nulls_with_0_join_to_timespine
    FROM (
      SELECT
        subq_8.metric_time__day AS metric_time__day
        , subq_4.__visits_fill_nulls_with_0_join_to_timespine AS __visits_fill_nulls_with_0_join_to_timespine
      FROM (
        SELECT
          subq_7.metric_time__day
        FROM (
          SELECT
            subq_6.metric_time__day
          FROM (
            SELECT
              subq_5.ds__day AS metric_time__day
              , subq_5.ds__week
              , subq_5.ds__month
              , subq_5.ds__quarter
              , subq_5.ds__year
              , subq_5.ds__extract_year
              , subq_5.ds__extract_quarter
              , subq_5.ds__extract_month
              , subq_5.ds__extract_day
              , subq_5.ds__extract_dow
              , subq_5.ds__extract_doy
              , subq_5.ds__alien_day
            FROM (
              SELECT
                time_spine_src_28006.ds AS ds__day
                , toStartOfWeek(time_spine_src_28006.ds, 1) AS ds__week
                , toStartOfMonth(time_spine_src_28006.ds) AS ds__month
                , toStartOfQuarter(time_spine_src_28006.ds) AS ds__quarter
                , toStartOfYear(time_spine_src_28006.ds) AS ds__year
                , toYear(time_spine_src_28006.ds) AS ds__extract_year
                , toQuarter(time_spine_src_28006.ds) AS ds__extract_quarter
                , toMonth(time_spine_src_28006.ds) AS ds__extract_month
                , toDayOfMonth(time_spine_src_28006.ds) AS ds__extract_day
                , toDayOfWeek(time_spine_src_28006.ds) AS ds__extract_dow
                , toDayOfYear(time_spine_src_28006.ds) AS ds__extract_doy
                , time_spine_src_28006.alien_day AS ds__alien_day
              FROM ***************************.mf_time_spine time_spine_src_28006
            ) subq_5
          ) subq_6
        ) subq_7
      ) subq_8
      LEFT OUTER JOIN (
        SELECT
          subq_3.metric_time__day
          , SUM(subq_3.__visits_fill_nulls_with_0_join_to_timespine) AS __visits_fill_nulls_with_0_join_to_timespine
        FROM (
          SELECT
            subq_2.metric_time__day
            , subq_2.__visits_fill_nulls_with_0_join_to_timespine
          FROM (
            SELECT
              subq_1.metric_time__day
              , subq_1.__visits_fill_nulls_with_0_join_to_timespine
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
                , subq_0.visit__ds__day
                , subq_0.visit__ds__week
                , subq_0.visit__ds__month
                , subq_0.visit__ds__quarter
                , subq_0.visit__ds__year
                , subq_0.visit__ds__extract_year
                , subq_0.visit__ds__extract_quarter
                , subq_0.visit__ds__extract_month
                , subq_0.visit__ds__extract_day
                , subq_0.visit__ds__extract_dow
                , subq_0.visit__ds__extract_doy
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
                , subq_0.session
                , subq_0.visit__user
                , subq_0.visit__session
                , subq_0.referrer_id
                , subq_0.visit__referrer_id
                , subq_0.__visits
                , subq_0.__visits_fill_nulls_with_0_join_to_timespine
              FROM (
                SELECT
                  1 AS __visits
                  , 1 AS __visits_fill_nulls_with_0_join_to_timespine
                  , toStartOfDay(visits_source_src_28000.ds) AS ds__day
                  , toStartOfWeek(visits_source_src_28000.ds, 1) AS ds__week
                  , toStartOfMonth(visits_source_src_28000.ds) AS ds__month
                  , toStartOfQuarter(visits_source_src_28000.ds) AS ds__quarter
                  , toStartOfYear(visits_source_src_28000.ds) AS ds__year
                  , toYear(visits_source_src_28000.ds) AS ds__extract_year
                  , toQuarter(visits_source_src_28000.ds) AS ds__extract_quarter
                  , toMonth(visits_source_src_28000.ds) AS ds__extract_month
                  , toDayOfMonth(visits_source_src_28000.ds) AS ds__extract_day
                  , toDayOfWeek(visits_source_src_28000.ds) AS ds__extract_dow
                  , toDayOfYear(visits_source_src_28000.ds) AS ds__extract_doy
                  , visits_source_src_28000.referrer_id
                  , toStartOfDay(visits_source_src_28000.ds) AS visit__ds__day
                  , toStartOfWeek(visits_source_src_28000.ds, 1) AS visit__ds__week
                  , toStartOfMonth(visits_source_src_28000.ds) AS visit__ds__month
                  , toStartOfQuarter(visits_source_src_28000.ds) AS visit__ds__quarter
                  , toStartOfYear(visits_source_src_28000.ds) AS visit__ds__year
                  , toYear(visits_source_src_28000.ds) AS visit__ds__extract_year
                  , toQuarter(visits_source_src_28000.ds) AS visit__ds__extract_quarter
                  , toMonth(visits_source_src_28000.ds) AS visit__ds__extract_month
                  , toDayOfMonth(visits_source_src_28000.ds) AS visit__ds__extract_day
                  , toDayOfWeek(visits_source_src_28000.ds) AS visit__ds__extract_dow
                  , toDayOfYear(visits_source_src_28000.ds) AS visit__ds__extract_doy
                  , visits_source_src_28000.referrer_id AS visit__referrer_id
                  , visits_source_src_28000.user_id AS user
                  , visits_source_src_28000.session_id AS session
                  , visits_source_src_28000.user_id AS visit__user
                  , visits_source_src_28000.session_id AS visit__session
                FROM ***************************.fct_visits visits_source_src_28000
              ) subq_0
            ) subq_1
          ) subq_2
        ) subq_3
        GROUP BY
          subq_3.metric_time__day
      ) subq_4
      ON
        subq_8.metric_time__day = subq_4.metric_time__day
    ) subq_9
    FULL OUTER JOIN (
      SELECT
        subq_24.metric_time__day AS metric_time__day
        , subq_20.__buys_fill_nulls_with_0_join_to_timespine AS __buys_fill_nulls_with_0_join_to_timespine
      FROM (
        SELECT
          subq_23.metric_time__day
        FROM (
          SELECT
            subq_22.metric_time__day
          FROM (
            SELECT
              subq_21.ds__day AS metric_time__day
              , subq_21.ds__week
              , subq_21.ds__month
              , subq_21.ds__quarter
              , subq_21.ds__year
              , subq_21.ds__extract_year
              , subq_21.ds__extract_quarter
              , subq_21.ds__extract_month
              , subq_21.ds__extract_day
              , subq_21.ds__extract_dow
              , subq_21.ds__extract_doy
              , subq_21.ds__alien_day
            FROM (
              SELECT
                time_spine_src_28006.ds AS ds__day
                , toStartOfWeek(time_spine_src_28006.ds, 1) AS ds__week
                , toStartOfMonth(time_spine_src_28006.ds) AS ds__month
                , toStartOfQuarter(time_spine_src_28006.ds) AS ds__quarter
                , toStartOfYear(time_spine_src_28006.ds) AS ds__year
                , toYear(time_spine_src_28006.ds) AS ds__extract_year
                , toQuarter(time_spine_src_28006.ds) AS ds__extract_quarter
                , toMonth(time_spine_src_28006.ds) AS ds__extract_month
                , toDayOfMonth(time_spine_src_28006.ds) AS ds__extract_day
                , toDayOfWeek(time_spine_src_28006.ds) AS ds__extract_dow
                , toDayOfYear(time_spine_src_28006.ds) AS ds__extract_doy
                , time_spine_src_28006.alien_day AS ds__alien_day
              FROM ***************************.mf_time_spine time_spine_src_28006
            ) subq_21
          ) subq_22
        ) subq_23
      ) subq_24
      LEFT OUTER JOIN (
        SELECT
          subq_19.metric_time__day
          , SUM(subq_19.__buys_fill_nulls_with_0_join_to_timespine) AS __buys_fill_nulls_with_0_join_to_timespine
        FROM (
          SELECT
            subq_18.metric_time__day
            , subq_18.__buys_fill_nulls_with_0_join_to_timespine
          FROM (
            SELECT
              subq_17.metric_time__day
              , subq_17.__buys_fill_nulls_with_0_join_to_timespine
            FROM (
              SELECT
                subq_16.metric_time__day
                , subq_16.user
                , subq_16.__buys_fill_nulls_with_0_join_to_timespine
                , subq_16.__visits_fill_nulls_with_0_join_to_timespine
              FROM (
                SELECT DISTINCT
                  FIRST_VALUE(subq_12.__visits_fill_nulls_with_0_join_to_timespine) OVER (
                    PARTITION BY
                      subq_15.user
                      , subq_15.metric_time__day
                      , subq_15.mf_internal_uuid
                    ORDER BY subq_12.metric_time__day DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                  ) AS __visits_fill_nulls_with_0_join_to_timespine
                  , FIRST_VALUE(subq_12.metric_time__day) OVER (
                    PARTITION BY
                      subq_15.user
                      , subq_15.metric_time__day
                      , subq_15.mf_internal_uuid
                    ORDER BY subq_12.metric_time__day DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                  ) AS metric_time__day
                  , FIRST_VALUE(subq_12.user) OVER (
                    PARTITION BY
                      subq_15.user
                      , subq_15.metric_time__day
                      , subq_15.mf_internal_uuid
                    ORDER BY subq_12.metric_time__day DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                  ) AS user
                  , subq_15.mf_internal_uuid AS mf_internal_uuid
                  , subq_15.__buys_fill_nulls_with_0_join_to_timespine AS __buys_fill_nulls_with_0_join_to_timespine
                FROM (
                  SELECT
                    subq_11.metric_time__day
                    , subq_11.user
                    , subq_11.__visits_fill_nulls_with_0_join_to_timespine
                  FROM (
                    SELECT
                      subq_10.metric_time__day
                      , subq_10.user
                      , subq_10.__visits_fill_nulls_with_0_join_to_timespine
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
                        , subq_0.visit__ds__day
                        , subq_0.visit__ds__week
                        , subq_0.visit__ds__month
                        , subq_0.visit__ds__quarter
                        , subq_0.visit__ds__year
                        , subq_0.visit__ds__extract_year
                        , subq_0.visit__ds__extract_quarter
                        , subq_0.visit__ds__extract_month
                        , subq_0.visit__ds__extract_day
                        , subq_0.visit__ds__extract_dow
                        , subq_0.visit__ds__extract_doy
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
                        , subq_0.session
                        , subq_0.visit__user
                        , subq_0.visit__session
                        , subq_0.referrer_id
                        , subq_0.visit__referrer_id
                        , subq_0.__visits
                        , subq_0.__visits_fill_nulls_with_0_join_to_timespine
                      FROM (
                        SELECT
                          1 AS __visits
                          , 1 AS __visits_fill_nulls_with_0_join_to_timespine
                          , toStartOfDay(visits_source_src_28000.ds) AS ds__day
                          , toStartOfWeek(visits_source_src_28000.ds, 1) AS ds__week
                          , toStartOfMonth(visits_source_src_28000.ds) AS ds__month
                          , toStartOfQuarter(visits_source_src_28000.ds) AS ds__quarter
                          , toStartOfYear(visits_source_src_28000.ds) AS ds__year
                          , toYear(visits_source_src_28000.ds) AS ds__extract_year
                          , toQuarter(visits_source_src_28000.ds) AS ds__extract_quarter
                          , toMonth(visits_source_src_28000.ds) AS ds__extract_month
                          , toDayOfMonth(visits_source_src_28000.ds) AS ds__extract_day
                          , toDayOfWeek(visits_source_src_28000.ds) AS ds__extract_dow
                          , toDayOfYear(visits_source_src_28000.ds) AS ds__extract_doy
                          , visits_source_src_28000.referrer_id
                          , toStartOfDay(visits_source_src_28000.ds) AS visit__ds__day
                          , toStartOfWeek(visits_source_src_28000.ds, 1) AS visit__ds__week
                          , toStartOfMonth(visits_source_src_28000.ds) AS visit__ds__month
                          , toStartOfQuarter(visits_source_src_28000.ds) AS visit__ds__quarter
                          , toStartOfYear(visits_source_src_28000.ds) AS visit__ds__year
                          , toYear(visits_source_src_28000.ds) AS visit__ds__extract_year
                          , toQuarter(visits_source_src_28000.ds) AS visit__ds__extract_quarter
                          , toMonth(visits_source_src_28000.ds) AS visit__ds__extract_month
                          , toDayOfMonth(visits_source_src_28000.ds) AS visit__ds__extract_day
                          , toDayOfWeek(visits_source_src_28000.ds) AS visit__ds__extract_dow
                          , toDayOfYear(visits_source_src_28000.ds) AS visit__ds__extract_doy
                          , visits_source_src_28000.referrer_id AS visit__referrer_id
                          , visits_source_src_28000.user_id AS user
                          , visits_source_src_28000.session_id AS session
                          , visits_source_src_28000.user_id AS visit__user
                          , visits_source_src_28000.session_id AS visit__session
                        FROM ***************************.fct_visits visits_source_src_28000
                      ) subq_0
                    ) subq_10
                  ) subq_11
                ) subq_12
                INNER JOIN (
                  SELECT
                    subq_14.ds__day
                    , subq_14.ds__week
                    , subq_14.ds__month
                    , subq_14.ds__quarter
                    , subq_14.ds__year
                    , subq_14.ds__extract_year
                    , subq_14.ds__extract_quarter
                    , subq_14.ds__extract_month
                    , subq_14.ds__extract_day
                    , subq_14.ds__extract_dow
                    , subq_14.ds__extract_doy
                    , subq_14.ds_month__month
                    , subq_14.ds_month__quarter
                    , subq_14.ds_month__year
                    , subq_14.ds_month__extract_year
                    , subq_14.ds_month__extract_quarter
                    , subq_14.ds_month__extract_month
                    , subq_14.buy__ds__day
                    , subq_14.buy__ds__week
                    , subq_14.buy__ds__month
                    , subq_14.buy__ds__quarter
                    , subq_14.buy__ds__year
                    , subq_14.buy__ds__extract_year
                    , subq_14.buy__ds__extract_quarter
                    , subq_14.buy__ds__extract_month
                    , subq_14.buy__ds__extract_day
                    , subq_14.buy__ds__extract_dow
                    , subq_14.buy__ds__extract_doy
                    , subq_14.buy__ds_month__month
                    , subq_14.buy__ds_month__quarter
                    , subq_14.buy__ds_month__year
                    , subq_14.buy__ds_month__extract_year
                    , subq_14.buy__ds_month__extract_quarter
                    , subq_14.buy__ds_month__extract_month
                    , subq_14.metric_time__day
                    , subq_14.metric_time__week
                    , subq_14.metric_time__month
                    , subq_14.metric_time__quarter
                    , subq_14.metric_time__year
                    , subq_14.metric_time__extract_year
                    , subq_14.metric_time__extract_quarter
                    , subq_14.metric_time__extract_month
                    , subq_14.metric_time__extract_day
                    , subq_14.metric_time__extract_dow
                    , subq_14.metric_time__extract_doy
                    , subq_14.user
                    , subq_14.session_id
                    , subq_14.buy__user
                    , subq_14.buy__session_id
                    , subq_14.__buys
                    , subq_14.__buys_fill_nulls_with_0
                    , subq_14.__buys_fill_nulls_with_0_join_to_timespine
                    , generateUUIDv4() AS mf_internal_uuid
                  FROM (
                    SELECT
                      subq_13.ds__day
                      , subq_13.ds__week
                      , subq_13.ds__month
                      , subq_13.ds__quarter
                      , subq_13.ds__year
                      , subq_13.ds__extract_year
                      , subq_13.ds__extract_quarter
                      , subq_13.ds__extract_month
                      , subq_13.ds__extract_day
                      , subq_13.ds__extract_dow
                      , subq_13.ds__extract_doy
                      , subq_13.ds_month__month
                      , subq_13.ds_month__quarter
                      , subq_13.ds_month__year
                      , subq_13.ds_month__extract_year
                      , subq_13.ds_month__extract_quarter
                      , subq_13.ds_month__extract_month
                      , subq_13.buy__ds__day
                      , subq_13.buy__ds__week
                      , subq_13.buy__ds__month
                      , subq_13.buy__ds__quarter
                      , subq_13.buy__ds__year
                      , subq_13.buy__ds__extract_year
                      , subq_13.buy__ds__extract_quarter
                      , subq_13.buy__ds__extract_month
                      , subq_13.buy__ds__extract_day
                      , subq_13.buy__ds__extract_dow
                      , subq_13.buy__ds__extract_doy
                      , subq_13.buy__ds_month__month
                      , subq_13.buy__ds_month__quarter
                      , subq_13.buy__ds_month__year
                      , subq_13.buy__ds_month__extract_year
                      , subq_13.buy__ds_month__extract_quarter
                      , subq_13.buy__ds_month__extract_month
                      , subq_13.ds__day AS metric_time__day
                      , subq_13.ds__week AS metric_time__week
                      , subq_13.ds__month AS metric_time__month
                      , subq_13.ds__quarter AS metric_time__quarter
                      , subq_13.ds__year AS metric_time__year
                      , subq_13.ds__extract_year AS metric_time__extract_year
                      , subq_13.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_13.ds__extract_month AS metric_time__extract_month
                      , subq_13.ds__extract_day AS metric_time__extract_day
                      , subq_13.ds__extract_dow AS metric_time__extract_dow
                      , subq_13.ds__extract_doy AS metric_time__extract_doy
                      , subq_13.user
                      , subq_13.session_id
                      , subq_13.buy__user
                      , subq_13.buy__session_id
                      , subq_13.__buys
                      , subq_13.__buys_fill_nulls_with_0
                      , subq_13.__buys_fill_nulls_with_0_join_to_timespine
                    FROM (
                      SELECT
                        1 AS __buys
                        , 1 AS __buys_fill_nulls_with_0
                        , 1 AS __buys_fill_nulls_with_0_join_to_timespine
                        , 1 AS __buys_month
                        , toStartOfDay(buys_source_src_28000.ds) AS ds__day
                        , toStartOfWeek(buys_source_src_28000.ds, 1) AS ds__week
                        , toStartOfMonth(buys_source_src_28000.ds) AS ds__month
                        , toStartOfQuarter(buys_source_src_28000.ds) AS ds__quarter
                        , toStartOfYear(buys_source_src_28000.ds) AS ds__year
                        , toYear(buys_source_src_28000.ds) AS ds__extract_year
                        , toQuarter(buys_source_src_28000.ds) AS ds__extract_quarter
                        , toMonth(buys_source_src_28000.ds) AS ds__extract_month
                        , toDayOfMonth(buys_source_src_28000.ds) AS ds__extract_day
                        , toDayOfWeek(buys_source_src_28000.ds) AS ds__extract_dow
                        , toDayOfYear(buys_source_src_28000.ds) AS ds__extract_doy
                        , toStartOfMonth(buys_source_src_28000.ds_month) AS ds_month__month
                        , toStartOfQuarter(buys_source_src_28000.ds_month) AS ds_month__quarter
                        , toStartOfYear(buys_source_src_28000.ds_month) AS ds_month__year
                        , toYear(buys_source_src_28000.ds_month) AS ds_month__extract_year
                        , toQuarter(buys_source_src_28000.ds_month) AS ds_month__extract_quarter
                        , toMonth(buys_source_src_28000.ds_month) AS ds_month__extract_month
                        , toStartOfDay(buys_source_src_28000.ds) AS buy__ds__day
                        , toStartOfWeek(buys_source_src_28000.ds, 1) AS buy__ds__week
                        , toStartOfMonth(buys_source_src_28000.ds) AS buy__ds__month
                        , toStartOfQuarter(buys_source_src_28000.ds) AS buy__ds__quarter
                        , toStartOfYear(buys_source_src_28000.ds) AS buy__ds__year
                        , toYear(buys_source_src_28000.ds) AS buy__ds__extract_year
                        , toQuarter(buys_source_src_28000.ds) AS buy__ds__extract_quarter
                        , toMonth(buys_source_src_28000.ds) AS buy__ds__extract_month
                        , toDayOfMonth(buys_source_src_28000.ds) AS buy__ds__extract_day
                        , toDayOfWeek(buys_source_src_28000.ds) AS buy__ds__extract_dow
                        , toDayOfYear(buys_source_src_28000.ds) AS buy__ds__extract_doy
                        , toStartOfMonth(buys_source_src_28000.ds_month) AS buy__ds_month__month
                        , toStartOfQuarter(buys_source_src_28000.ds_month) AS buy__ds_month__quarter
                        , toStartOfYear(buys_source_src_28000.ds_month) AS buy__ds_month__year
                        , toYear(buys_source_src_28000.ds_month) AS buy__ds_month__extract_year
                        , toQuarter(buys_source_src_28000.ds_month) AS buy__ds_month__extract_quarter
                        , toMonth(buys_source_src_28000.ds_month) AS buy__ds_month__extract_month
                        , buys_source_src_28000.user_id AS user
                        , buys_source_src_28000.session_id
                        , buys_source_src_28000.user_id AS buy__user
                        , buys_source_src_28000.session_id AS buy__session_id
                      FROM ***************************.fct_buys buys_source_src_28000
                    ) subq_13
                  ) subq_14
                ) subq_15
                ON
                  (
                    subq_12.user = subq_15.user
                  ) AND (
                    (
                      subq_12.metric_time__day <= subq_15.metric_time__day
                    ) AND (
                      subq_12.metric_time__day > addDays(subq_15.metric_time__day, -7)
                    )
                  )
              ) subq_16
            ) subq_17
          ) subq_18
        ) subq_19
        GROUP BY
          subq_19.metric_time__day
      ) subq_20
      ON
        subq_24.metric_time__day = subq_20.metric_time__day
    ) subq_25
    ON
      subq_9.metric_time__day = subq_25.metric_time__day
    GROUP BY
      COALESCE(subq_9.metric_time__day, subq_25.metric_time__day)
  ) subq_26
) subq_27
