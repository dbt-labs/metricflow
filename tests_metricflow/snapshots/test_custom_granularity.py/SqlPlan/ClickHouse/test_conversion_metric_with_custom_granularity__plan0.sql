test_name: test_conversion_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: ClickHouse
---
SELECT
  subq_19.metric_time__alien_day
  , subq_19.visit_buy_conversion_rate_7days
FROM (
  SELECT
    subq_18.metric_time__alien_day
    , CAST(subq_18.__buys AS Nullable(Float64)) / CAST(NULLIF(subq_18.__visits, 0) AS Nullable(Float64)) AS visit_buy_conversion_rate_7days
  FROM (
    SELECT
      COALESCE(subq_5.metric_time__alien_day, subq_17.metric_time__alien_day) AS metric_time__alien_day
      , MAX(subq_5.__visits) AS __visits
      , MAX(subq_17.__buys) AS __buys
    FROM (
      SELECT
        subq_4.metric_time__alien_day
        , SUM(subq_4.__visits) AS __visits
      FROM (
        SELECT
          subq_3.metric_time__alien_day
          , subq_3.__visits
        FROM (
          SELECT
            subq_2.metric_time__alien_day
            , subq_2.__visits
          FROM (
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
              , subq_0.visit__ds__day AS visit__ds__day
              , subq_0.visit__ds__week AS visit__ds__week
              , subq_0.visit__ds__month AS visit__ds__month
              , subq_0.visit__ds__quarter AS visit__ds__quarter
              , subq_0.visit__ds__year AS visit__ds__year
              , subq_0.visit__ds__extract_year AS visit__ds__extract_year
              , subq_0.visit__ds__extract_quarter AS visit__ds__extract_quarter
              , subq_0.visit__ds__extract_month AS visit__ds__extract_month
              , subq_0.visit__ds__extract_day AS visit__ds__extract_day
              , subq_0.visit__ds__extract_dow AS visit__ds__extract_dow
              , subq_0.visit__ds__extract_doy AS visit__ds__extract_doy
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
              , subq_0.user AS user
              , subq_0.session AS session
              , subq_0.visit__user AS visit__user
              , subq_0.visit__session AS visit__session
              , subq_0.referrer_id AS referrer_id
              , subq_0.visit__referrer_id AS visit__referrer_id
              , subq_0.__visits AS __visits
              , subq_0.__visits_fill_nulls_with_0_join_to_timespine AS __visits_fill_nulls_with_0_join_to_timespine
              , subq_1.alien_day AS metric_time__alien_day
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
            LEFT OUTER JOIN
              ***************************.mf_time_spine subq_1
            ON
              subq_0.ds__day = subq_1.ds
          ) subq_2
        ) subq_3
      ) subq_4
      GROUP BY
        subq_4.metric_time__alien_day
    ) subq_5
    FULL OUTER JOIN (
      SELECT
        subq_16.metric_time__alien_day
        , SUM(subq_16.__buys) AS __buys
      FROM (
        SELECT
          subq_15.metric_time__alien_day
          , subq_15.__buys
        FROM (
          SELECT
            subq_14.metric_time__alien_day
            , subq_14.__buys
          FROM (
            SELECT
              subq_13.metric_time__alien_day
              , subq_13.metric_time__day
              , subq_13.user
              , subq_13.__buys
              , subq_13.__visits
            FROM (
              SELECT DISTINCT
                FIRST_VALUE(subq_9.__visits) OVER (
                  PARTITION BY
                    subq_12.user
                    , subq_12.metric_time__day
                    , subq_12.mf_internal_uuid
                  ORDER BY subq_9.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS __visits
                , FIRST_VALUE(subq_9.metric_time__alien_day) OVER (
                  PARTITION BY
                    subq_12.user
                    , subq_12.metric_time__day
                    , subq_12.mf_internal_uuid
                  ORDER BY subq_9.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS metric_time__alien_day
                , FIRST_VALUE(subq_9.metric_time__day) OVER (
                  PARTITION BY
                    subq_12.user
                    , subq_12.metric_time__day
                    , subq_12.mf_internal_uuid
                  ORDER BY subq_9.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS metric_time__day
                , FIRST_VALUE(subq_9.user) OVER (
                  PARTITION BY
                    subq_12.user
                    , subq_12.metric_time__day
                    , subq_12.mf_internal_uuid
                  ORDER BY subq_9.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS user
                , subq_12.mf_internal_uuid AS mf_internal_uuid
                , subq_12.__buys AS __buys
              FROM (
                SELECT
                  subq_8.metric_time__alien_day
                  , subq_8.metric_time__day
                  , subq_8.user
                  , subq_8.__visits
                FROM (
                  SELECT
                    subq_7.metric_time__alien_day
                    , subq_7.metric_time__day
                    , subq_7.user
                    , subq_7.__visits
                  FROM (
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
                      , subq_0.visit__ds__day AS visit__ds__day
                      , subq_0.visit__ds__week AS visit__ds__week
                      , subq_0.visit__ds__month AS visit__ds__month
                      , subq_0.visit__ds__quarter AS visit__ds__quarter
                      , subq_0.visit__ds__year AS visit__ds__year
                      , subq_0.visit__ds__extract_year AS visit__ds__extract_year
                      , subq_0.visit__ds__extract_quarter AS visit__ds__extract_quarter
                      , subq_0.visit__ds__extract_month AS visit__ds__extract_month
                      , subq_0.visit__ds__extract_day AS visit__ds__extract_day
                      , subq_0.visit__ds__extract_dow AS visit__ds__extract_dow
                      , subq_0.visit__ds__extract_doy AS visit__ds__extract_doy
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
                      , subq_0.user AS user
                      , subq_0.session AS session
                      , subq_0.visit__user AS visit__user
                      , subq_0.visit__session AS visit__session
                      , subq_0.referrer_id AS referrer_id
                      , subq_0.visit__referrer_id AS visit__referrer_id
                      , subq_0.__visits AS __visits
                      , subq_0.__visits_fill_nulls_with_0_join_to_timespine AS __visits_fill_nulls_with_0_join_to_timespine
                      , subq_6.alien_day AS metric_time__alien_day
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
                    LEFT OUTER JOIN
                      ***************************.mf_time_spine subq_6
                    ON
                      subq_0.ds__day = subq_6.ds
                  ) subq_7
                ) subq_8
              ) subq_9
              INNER JOIN (
                SELECT
                  subq_11.ds__day
                  , subq_11.ds__week
                  , subq_11.ds__month
                  , subq_11.ds__quarter
                  , subq_11.ds__year
                  , subq_11.ds__extract_year
                  , subq_11.ds__extract_quarter
                  , subq_11.ds__extract_month
                  , subq_11.ds__extract_day
                  , subq_11.ds__extract_dow
                  , subq_11.ds__extract_doy
                  , subq_11.ds_month__month
                  , subq_11.ds_month__quarter
                  , subq_11.ds_month__year
                  , subq_11.ds_month__extract_year
                  , subq_11.ds_month__extract_quarter
                  , subq_11.ds_month__extract_month
                  , subq_11.buy__ds__day
                  , subq_11.buy__ds__week
                  , subq_11.buy__ds__month
                  , subq_11.buy__ds__quarter
                  , subq_11.buy__ds__year
                  , subq_11.buy__ds__extract_year
                  , subq_11.buy__ds__extract_quarter
                  , subq_11.buy__ds__extract_month
                  , subq_11.buy__ds__extract_day
                  , subq_11.buy__ds__extract_dow
                  , subq_11.buy__ds__extract_doy
                  , subq_11.buy__ds_month__month
                  , subq_11.buy__ds_month__quarter
                  , subq_11.buy__ds_month__year
                  , subq_11.buy__ds_month__extract_year
                  , subq_11.buy__ds_month__extract_quarter
                  , subq_11.buy__ds_month__extract_month
                  , subq_11.metric_time__day
                  , subq_11.metric_time__week
                  , subq_11.metric_time__month
                  , subq_11.metric_time__quarter
                  , subq_11.metric_time__year
                  , subq_11.metric_time__extract_year
                  , subq_11.metric_time__extract_quarter
                  , subq_11.metric_time__extract_month
                  , subq_11.metric_time__extract_day
                  , subq_11.metric_time__extract_dow
                  , subq_11.metric_time__extract_doy
                  , subq_11.user
                  , subq_11.session_id
                  , subq_11.buy__user
                  , subq_11.buy__session_id
                  , subq_11.__buys
                  , subq_11.__buys_fill_nulls_with_0
                  , subq_11.__buys_fill_nulls_with_0_join_to_timespine
                  , generateUUIDv4() AS mf_internal_uuid
                FROM (
                  SELECT
                    subq_10.ds__day
                    , subq_10.ds__week
                    , subq_10.ds__month
                    , subq_10.ds__quarter
                    , subq_10.ds__year
                    , subq_10.ds__extract_year
                    , subq_10.ds__extract_quarter
                    , subq_10.ds__extract_month
                    , subq_10.ds__extract_day
                    , subq_10.ds__extract_dow
                    , subq_10.ds__extract_doy
                    , subq_10.ds_month__month
                    , subq_10.ds_month__quarter
                    , subq_10.ds_month__year
                    , subq_10.ds_month__extract_year
                    , subq_10.ds_month__extract_quarter
                    , subq_10.ds_month__extract_month
                    , subq_10.buy__ds__day
                    , subq_10.buy__ds__week
                    , subq_10.buy__ds__month
                    , subq_10.buy__ds__quarter
                    , subq_10.buy__ds__year
                    , subq_10.buy__ds__extract_year
                    , subq_10.buy__ds__extract_quarter
                    , subq_10.buy__ds__extract_month
                    , subq_10.buy__ds__extract_day
                    , subq_10.buy__ds__extract_dow
                    , subq_10.buy__ds__extract_doy
                    , subq_10.buy__ds_month__month
                    , subq_10.buy__ds_month__quarter
                    , subq_10.buy__ds_month__year
                    , subq_10.buy__ds_month__extract_year
                    , subq_10.buy__ds_month__extract_quarter
                    , subq_10.buy__ds_month__extract_month
                    , subq_10.ds__day AS metric_time__day
                    , subq_10.ds__week AS metric_time__week
                    , subq_10.ds__month AS metric_time__month
                    , subq_10.ds__quarter AS metric_time__quarter
                    , subq_10.ds__year AS metric_time__year
                    , subq_10.ds__extract_year AS metric_time__extract_year
                    , subq_10.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_10.ds__extract_month AS metric_time__extract_month
                    , subq_10.ds__extract_day AS metric_time__extract_day
                    , subq_10.ds__extract_dow AS metric_time__extract_dow
                    , subq_10.ds__extract_doy AS metric_time__extract_doy
                    , subq_10.user
                    , subq_10.session_id
                    , subq_10.buy__user
                    , subq_10.buy__session_id
                    , subq_10.__buys
                    , subq_10.__buys_fill_nulls_with_0
                    , subq_10.__buys_fill_nulls_with_0_join_to_timespine
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
                  ) subq_10
                ) subq_11
              ) subq_12
              ON
                (
                  subq_9.user = subq_12.user
                ) AND (
                  (
                    subq_9.metric_time__day <= subq_12.metric_time__day
                  ) AND (
                    subq_9.metric_time__day > addDays(subq_12.metric_time__day, -7)
                  )
                )
            ) subq_13
          ) subq_14
        ) subq_15
      ) subq_16
      GROUP BY
        subq_16.metric_time__alien_day
    ) subq_17
    ON
      subq_5.metric_time__alien_day = subq_17.metric_time__alien_day
    GROUP BY
      COALESCE(subq_5.metric_time__alien_day, subq_17.metric_time__alien_day)
  ) subq_18
) subq_19
