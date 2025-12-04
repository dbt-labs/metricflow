test_name: test_conversion_metric_with_custom_granularity_filter
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Write to DataTable
SELECT
  subq_21.metric_time__alien_day
  , subq_21.visit_buy_conversion_rate_7days
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_20.metric_time__alien_day
    , CAST(subq_20.__buys AS DOUBLE) / CAST(NULLIF(subq_20.__visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_6.metric_time__alien_day, subq_19.metric_time__alien_day) AS metric_time__alien_day
      , MAX(subq_6.__visits) AS __visits
      , MAX(subq_19.__buys) AS __buys
    FROM (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_5.metric_time__alien_day
        , SUM(subq_5.__visits) AS __visits
      FROM (
        -- Pass Only Elements: ['__visits', 'metric_time__alien_day']
        SELECT
          subq_4.metric_time__alien_day
          , subq_4.__visits
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_3.visits AS __visits
            , subq_3.metric_time__alien_day
          FROM (
            -- Pass Only Elements: ['__visits', 'metric_time__alien_day']
            SELECT
              subq_2.metric_time__alien_day
              , subq_2.__visits AS visits
            FROM (
              -- Metric Time Dimension 'ds'
              -- Join to Custom Granularity Dataset
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
                -- Read Elements From Semantic Model 'visits_source'
                SELECT
                  1 AS __visits
                  , 1 AS __visits_fill_nulls_with_0_join_to_timespine
                  , DATE_TRUNC('day', visits_source_src_28000.ds) AS ds__day
                  , DATE_TRUNC('week', visits_source_src_28000.ds) AS ds__week
                  , DATE_TRUNC('month', visits_source_src_28000.ds) AS ds__month
                  , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS ds__quarter
                  , DATE_TRUNC('year', visits_source_src_28000.ds) AS ds__year
                  , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
                  , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
                  , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
                  , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
                  , EXTRACT(DAY_OF_WEEK FROM visits_source_src_28000.ds) AS ds__extract_dow
                  , EXTRACT(doy FROM visits_source_src_28000.ds) AS ds__extract_doy
                  , visits_source_src_28000.referrer_id
                  , DATE_TRUNC('day', visits_source_src_28000.ds) AS visit__ds__day
                  , DATE_TRUNC('week', visits_source_src_28000.ds) AS visit__ds__week
                  , DATE_TRUNC('month', visits_source_src_28000.ds) AS visit__ds__month
                  , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS visit__ds__quarter
                  , DATE_TRUNC('year', visits_source_src_28000.ds) AS visit__ds__year
                  , EXTRACT(year FROM visits_source_src_28000.ds) AS visit__ds__extract_year
                  , EXTRACT(quarter FROM visits_source_src_28000.ds) AS visit__ds__extract_quarter
                  , EXTRACT(month FROM visits_source_src_28000.ds) AS visit__ds__extract_month
                  , EXTRACT(day FROM visits_source_src_28000.ds) AS visit__ds__extract_day
                  , EXTRACT(DAY_OF_WEEK FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                  , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
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
          WHERE metric_time__alien_day = '2020-01-01'
        ) subq_4
      ) subq_5
      GROUP BY
        subq_5.metric_time__alien_day
    ) subq_6
    FULL OUTER JOIN (
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_18.metric_time__alien_day
        , SUM(subq_18.__buys) AS __buys
      FROM (
        -- Pass Only Elements: ['__buys', 'metric_time__alien_day']
        SELECT
          subq_17.metric_time__alien_day
          , subq_17.__buys
        FROM (
          -- Pass Only Elements: ['__buys', 'metric_time__alien_day']
          SELECT
            subq_16.metric_time__alien_day
            , subq_16.__buys
          FROM (
            -- Find conversions for user within the range of 7 day
            SELECT
              subq_15.metric_time__alien_day
              , subq_15.metric_time__day
              , subq_15.user
              , subq_15.__buys
              , subq_15.__visits
            FROM (
              -- Dedupe the fanout with mf_internal_uuid in the conversion data set
              SELECT DISTINCT
                FIRST_VALUE(subq_11.__visits) OVER (
                  PARTITION BY
                    subq_14.user
                    , subq_14.metric_time__day
                    , subq_14.mf_internal_uuid
                  ORDER BY subq_11.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS __visits
                , FIRST_VALUE(subq_11.metric_time__alien_day) OVER (
                  PARTITION BY
                    subq_14.user
                    , subq_14.metric_time__day
                    , subq_14.mf_internal_uuid
                  ORDER BY subq_11.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS metric_time__alien_day
                , FIRST_VALUE(subq_11.metric_time__day) OVER (
                  PARTITION BY
                    subq_14.user
                    , subq_14.metric_time__day
                    , subq_14.mf_internal_uuid
                  ORDER BY subq_11.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS metric_time__day
                , FIRST_VALUE(subq_11.user) OVER (
                  PARTITION BY
                    subq_14.user
                    , subq_14.metric_time__day
                    , subq_14.mf_internal_uuid
                  ORDER BY subq_11.metric_time__day DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS user
                , subq_14.mf_internal_uuid AS mf_internal_uuid
                , subq_14.__buys AS __buys
              FROM (
                -- Pass Only Elements: ['__visits', 'metric_time__day', 'metric_time__alien_day', 'user']
                SELECT
                  subq_10.metric_time__alien_day
                  , subq_10.metric_time__day
                  , subq_10.user
                  , subq_10.__visits
                FROM (
                  -- Constrain Output with WHERE
                  SELECT
                    subq_9.visits AS __visits
                    , subq_9.metric_time__alien_day
                    , subq_9.metric_time__day
                    , subq_9.user
                  FROM (
                    -- Pass Only Elements: ['__visits', 'metric_time__day', 'metric_time__alien_day', 'user']
                    SELECT
                      subq_8.metric_time__alien_day
                      , subq_8.metric_time__day
                      , subq_8.user
                      , subq_8.__visits AS visits
                    FROM (
                      -- Metric Time Dimension 'ds'
                      -- Join to Custom Granularity Dataset
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
                        , subq_7.alien_day AS metric_time__alien_day
                      FROM (
                        -- Read Elements From Semantic Model 'visits_source'
                        SELECT
                          1 AS __visits
                          , 1 AS __visits_fill_nulls_with_0_join_to_timespine
                          , DATE_TRUNC('day', visits_source_src_28000.ds) AS ds__day
                          , DATE_TRUNC('week', visits_source_src_28000.ds) AS ds__week
                          , DATE_TRUNC('month', visits_source_src_28000.ds) AS ds__month
                          , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS ds__quarter
                          , DATE_TRUNC('year', visits_source_src_28000.ds) AS ds__year
                          , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
                          , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
                          , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
                          , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
                          , EXTRACT(DAY_OF_WEEK FROM visits_source_src_28000.ds) AS ds__extract_dow
                          , EXTRACT(doy FROM visits_source_src_28000.ds) AS ds__extract_doy
                          , visits_source_src_28000.referrer_id
                          , DATE_TRUNC('day', visits_source_src_28000.ds) AS visit__ds__day
                          , DATE_TRUNC('week', visits_source_src_28000.ds) AS visit__ds__week
                          , DATE_TRUNC('month', visits_source_src_28000.ds) AS visit__ds__month
                          , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS visit__ds__quarter
                          , DATE_TRUNC('year', visits_source_src_28000.ds) AS visit__ds__year
                          , EXTRACT(year FROM visits_source_src_28000.ds) AS visit__ds__extract_year
                          , EXTRACT(quarter FROM visits_source_src_28000.ds) AS visit__ds__extract_quarter
                          , EXTRACT(month FROM visits_source_src_28000.ds) AS visit__ds__extract_month
                          , EXTRACT(day FROM visits_source_src_28000.ds) AS visit__ds__extract_day
                          , EXTRACT(DAY_OF_WEEK FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                          , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                          , visits_source_src_28000.referrer_id AS visit__referrer_id
                          , visits_source_src_28000.user_id AS user
                          , visits_source_src_28000.session_id AS session
                          , visits_source_src_28000.user_id AS visit__user
                          , visits_source_src_28000.session_id AS visit__session
                        FROM ***************************.fct_visits visits_source_src_28000
                      ) subq_0
                      LEFT OUTER JOIN
                        ***************************.mf_time_spine subq_7
                      ON
                        subq_0.ds__day = subq_7.ds
                    ) subq_8
                  ) subq_9
                  WHERE metric_time__alien_day = '2020-01-01'
                ) subq_10
              ) subq_11
              INNER JOIN (
                -- Add column with generated UUID
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
                  , subq_13.metric_time__day
                  , subq_13.metric_time__week
                  , subq_13.metric_time__month
                  , subq_13.metric_time__quarter
                  , subq_13.metric_time__year
                  , subq_13.metric_time__extract_year
                  , subq_13.metric_time__extract_quarter
                  , subq_13.metric_time__extract_month
                  , subq_13.metric_time__extract_day
                  , subq_13.metric_time__extract_dow
                  , subq_13.metric_time__extract_doy
                  , subq_13.user
                  , subq_13.session_id
                  , subq_13.buy__user
                  , subq_13.buy__session_id
                  , subq_13.__buys
                  , subq_13.__buys_fill_nulls_with_0
                  , subq_13.__buys_fill_nulls_with_0_join_to_timespine
                  , uuid() AS mf_internal_uuid
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_12.ds__day
                    , subq_12.ds__week
                    , subq_12.ds__month
                    , subq_12.ds__quarter
                    , subq_12.ds__year
                    , subq_12.ds__extract_year
                    , subq_12.ds__extract_quarter
                    , subq_12.ds__extract_month
                    , subq_12.ds__extract_day
                    , subq_12.ds__extract_dow
                    , subq_12.ds__extract_doy
                    , subq_12.ds_month__month
                    , subq_12.ds_month__quarter
                    , subq_12.ds_month__year
                    , subq_12.ds_month__extract_year
                    , subq_12.ds_month__extract_quarter
                    , subq_12.ds_month__extract_month
                    , subq_12.buy__ds__day
                    , subq_12.buy__ds__week
                    , subq_12.buy__ds__month
                    , subq_12.buy__ds__quarter
                    , subq_12.buy__ds__year
                    , subq_12.buy__ds__extract_year
                    , subq_12.buy__ds__extract_quarter
                    , subq_12.buy__ds__extract_month
                    , subq_12.buy__ds__extract_day
                    , subq_12.buy__ds__extract_dow
                    , subq_12.buy__ds__extract_doy
                    , subq_12.buy__ds_month__month
                    , subq_12.buy__ds_month__quarter
                    , subq_12.buy__ds_month__year
                    , subq_12.buy__ds_month__extract_year
                    , subq_12.buy__ds_month__extract_quarter
                    , subq_12.buy__ds_month__extract_month
                    , subq_12.ds__day AS metric_time__day
                    , subq_12.ds__week AS metric_time__week
                    , subq_12.ds__month AS metric_time__month
                    , subq_12.ds__quarter AS metric_time__quarter
                    , subq_12.ds__year AS metric_time__year
                    , subq_12.ds__extract_year AS metric_time__extract_year
                    , subq_12.ds__extract_quarter AS metric_time__extract_quarter
                    , subq_12.ds__extract_month AS metric_time__extract_month
                    , subq_12.ds__extract_day AS metric_time__extract_day
                    , subq_12.ds__extract_dow AS metric_time__extract_dow
                    , subq_12.ds__extract_doy AS metric_time__extract_doy
                    , subq_12.user
                    , subq_12.session_id
                    , subq_12.buy__user
                    , subq_12.buy__session_id
                    , subq_12.__buys
                    , subq_12.__buys_fill_nulls_with_0
                    , subq_12.__buys_fill_nulls_with_0_join_to_timespine
                  FROM (
                    -- Read Elements From Semantic Model 'buys_source'
                    SELECT
                      1 AS __buys
                      , 1 AS __buys_fill_nulls_with_0
                      , 1 AS __buys_fill_nulls_with_0_join_to_timespine
                      , 1 AS __buys_month
                      , DATE_TRUNC('day', buys_source_src_28000.ds) AS ds__day
                      , DATE_TRUNC('week', buys_source_src_28000.ds) AS ds__week
                      , DATE_TRUNC('month', buys_source_src_28000.ds) AS ds__month
                      , DATE_TRUNC('quarter', buys_source_src_28000.ds) AS ds__quarter
                      , DATE_TRUNC('year', buys_source_src_28000.ds) AS ds__year
                      , EXTRACT(year FROM buys_source_src_28000.ds) AS ds__extract_year
                      , EXTRACT(quarter FROM buys_source_src_28000.ds) AS ds__extract_quarter
                      , EXTRACT(month FROM buys_source_src_28000.ds) AS ds__extract_month
                      , EXTRACT(day FROM buys_source_src_28000.ds) AS ds__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM buys_source_src_28000.ds) AS ds__extract_dow
                      , EXTRACT(doy FROM buys_source_src_28000.ds) AS ds__extract_doy
                      , DATE_TRUNC('month', buys_source_src_28000.ds_month) AS ds_month__month
                      , DATE_TRUNC('quarter', buys_source_src_28000.ds_month) AS ds_month__quarter
                      , DATE_TRUNC('year', buys_source_src_28000.ds_month) AS ds_month__year
                      , EXTRACT(year FROM buys_source_src_28000.ds_month) AS ds_month__extract_year
                      , EXTRACT(quarter FROM buys_source_src_28000.ds_month) AS ds_month__extract_quarter
                      , EXTRACT(month FROM buys_source_src_28000.ds_month) AS ds_month__extract_month
                      , DATE_TRUNC('day', buys_source_src_28000.ds) AS buy__ds__day
                      , DATE_TRUNC('week', buys_source_src_28000.ds) AS buy__ds__week
                      , DATE_TRUNC('month', buys_source_src_28000.ds) AS buy__ds__month
                      , DATE_TRUNC('quarter', buys_source_src_28000.ds) AS buy__ds__quarter
                      , DATE_TRUNC('year', buys_source_src_28000.ds) AS buy__ds__year
                      , EXTRACT(year FROM buys_source_src_28000.ds) AS buy__ds__extract_year
                      , EXTRACT(quarter FROM buys_source_src_28000.ds) AS buy__ds__extract_quarter
                      , EXTRACT(month FROM buys_source_src_28000.ds) AS buy__ds__extract_month
                      , EXTRACT(day FROM buys_source_src_28000.ds) AS buy__ds__extract_day
                      , EXTRACT(DAY_OF_WEEK FROM buys_source_src_28000.ds) AS buy__ds__extract_dow
                      , EXTRACT(doy FROM buys_source_src_28000.ds) AS buy__ds__extract_doy
                      , DATE_TRUNC('month', buys_source_src_28000.ds_month) AS buy__ds_month__month
                      , DATE_TRUNC('quarter', buys_source_src_28000.ds_month) AS buy__ds_month__quarter
                      , DATE_TRUNC('year', buys_source_src_28000.ds_month) AS buy__ds_month__year
                      , EXTRACT(year FROM buys_source_src_28000.ds_month) AS buy__ds_month__extract_year
                      , EXTRACT(quarter FROM buys_source_src_28000.ds_month) AS buy__ds_month__extract_quarter
                      , EXTRACT(month FROM buys_source_src_28000.ds_month) AS buy__ds_month__extract_month
                      , buys_source_src_28000.user_id AS user
                      , buys_source_src_28000.session_id
                      , buys_source_src_28000.user_id AS buy__user
                      , buys_source_src_28000.session_id AS buy__session_id
                    FROM ***************************.fct_buys buys_source_src_28000
                  ) subq_12
                ) subq_13
              ) subq_14
              ON
                (
                  subq_11.user = subq_14.user
                ) AND (
                  (
                    subq_11.metric_time__day <= subq_14.metric_time__day
                  ) AND (
                    subq_11.metric_time__day > DATE_ADD('day', -7, subq_14.metric_time__day)
                  )
                )
            ) subq_15
          ) subq_16
        ) subq_17
      ) subq_18
      GROUP BY
        subq_18.metric_time__alien_day
    ) subq_19
    ON
      subq_6.metric_time__alien_day = subq_19.metric_time__alien_day
    GROUP BY
      COALESCE(subq_6.metric_time__alien_day, subq_19.metric_time__alien_day)
  ) subq_20
) subq_21
