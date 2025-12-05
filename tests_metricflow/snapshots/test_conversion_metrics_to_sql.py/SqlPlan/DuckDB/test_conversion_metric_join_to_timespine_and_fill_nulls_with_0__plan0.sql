test_name: test_conversion_metric_join_to_timespine_and_fill_nulls_with_0
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric that joins to time spine and fills nulls with 0.
sql_engine: DuckDB
---
-- Write to DataTable
SELECT
  subq_27.metric_time__day
  , subq_27.visit_buy_conversion_rate_7days_fill_nulls_with_0
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_26.metric_time__day
    , CAST(subq_26.__buys_fill_nulls_with_0_join_to_timespine AS DOUBLE) / CAST(NULLIF(subq_26.__visits_fill_nulls_with_0_join_to_timespine, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days_fill_nulls_with_0
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_9.metric_time__day, subq_25.metric_time__day) AS metric_time__day
      , COALESCE(MAX(subq_9.__visits_fill_nulls_with_0_join_to_timespine), 0) AS __visits_fill_nulls_with_0_join_to_timespine
      , COALESCE(MAX(subq_25.__buys_fill_nulls_with_0_join_to_timespine), 0) AS __buys_fill_nulls_with_0_join_to_timespine
    FROM (
      -- Join to Time Spine Dataset
      SELECT
        subq_8.metric_time__day AS metric_time__day
        , subq_4.__visits_fill_nulls_with_0_join_to_timespine AS __visits_fill_nulls_with_0_join_to_timespine
      FROM (
        -- Pass Only Elements: ['metric_time__day']
        SELECT
          subq_7.metric_time__day
        FROM (
          -- Pass Only Elements: ['metric_time__day']
          SELECT
            subq_6.metric_time__day
          FROM (
            -- Change Column Aliases
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
              -- Read From Time Spine 'mf_time_spine'
              SELECT
                time_spine_src_28006.ds AS ds__day
                , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
                , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
                , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
                , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
                , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
                , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
                , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
                , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
                , EXTRACT(isodow FROM time_spine_src_28006.ds) AS ds__extract_dow
                , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
                , time_spine_src_28006.alien_day AS ds__alien_day
              FROM ***************************.mf_time_spine time_spine_src_28006
            ) subq_5
          ) subq_6
        ) subq_7
      ) subq_8
      LEFT OUTER JOIN (
        -- Aggregate Inputs for Simple Metrics
        SELECT
          subq_3.metric_time__day
          , SUM(subq_3.__visits_fill_nulls_with_0_join_to_timespine) AS __visits_fill_nulls_with_0_join_to_timespine
        FROM (
          -- Pass Only Elements: ['__visits_fill_nulls_with_0_join_to_timespine', 'metric_time__day']
          SELECT
            subq_2.metric_time__day
            , subq_2.__visits_fill_nulls_with_0_join_to_timespine
          FROM (
            -- Pass Only Elements: ['__visits_fill_nulls_with_0_join_to_timespine', 'metric_time__day']
            SELECT
              subq_1.metric_time__day
              , subq_1.__visits_fill_nulls_with_0_join_to_timespine
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
                  , EXTRACT(isodow FROM visits_source_src_28000.ds) AS ds__extract_dow
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
                  , EXTRACT(isodow FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                  , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
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
      -- Join to Time Spine Dataset
      SELECT
        subq_24.metric_time__day AS metric_time__day
        , subq_20.__buys_fill_nulls_with_0_join_to_timespine AS __buys_fill_nulls_with_0_join_to_timespine
      FROM (
        -- Pass Only Elements: ['metric_time__day']
        SELECT
          subq_23.metric_time__day
        FROM (
          -- Pass Only Elements: ['metric_time__day']
          SELECT
            subq_22.metric_time__day
          FROM (
            -- Change Column Aliases
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
              -- Read From Time Spine 'mf_time_spine'
              SELECT
                time_spine_src_28006.ds AS ds__day
                , DATE_TRUNC('week', time_spine_src_28006.ds) AS ds__week
                , DATE_TRUNC('month', time_spine_src_28006.ds) AS ds__month
                , DATE_TRUNC('quarter', time_spine_src_28006.ds) AS ds__quarter
                , DATE_TRUNC('year', time_spine_src_28006.ds) AS ds__year
                , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
                , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
                , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
                , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
                , EXTRACT(isodow FROM time_spine_src_28006.ds) AS ds__extract_dow
                , EXTRACT(doy FROM time_spine_src_28006.ds) AS ds__extract_doy
                , time_spine_src_28006.alien_day AS ds__alien_day
              FROM ***************************.mf_time_spine time_spine_src_28006
            ) subq_21
          ) subq_22
        ) subq_23
      ) subq_24
      LEFT OUTER JOIN (
        -- Aggregate Inputs for Simple Metrics
        SELECT
          subq_19.metric_time__day
          , SUM(subq_19.__buys_fill_nulls_with_0_join_to_timespine) AS __buys_fill_nulls_with_0_join_to_timespine
        FROM (
          -- Pass Only Elements: ['__buys_fill_nulls_with_0_join_to_timespine', 'metric_time__day']
          SELECT
            subq_18.metric_time__day
            , subq_18.__buys_fill_nulls_with_0_join_to_timespine
          FROM (
            -- Pass Only Elements: ['__buys_fill_nulls_with_0_join_to_timespine', 'metric_time__day']
            SELECT
              subq_17.metric_time__day
              , subq_17.__buys_fill_nulls_with_0_join_to_timespine
            FROM (
              -- Find conversions for user within the range of 7 day
              SELECT
                subq_16.metric_time__day
                , subq_16.user
                , subq_16.__buys_fill_nulls_with_0_join_to_timespine
                , subq_16.__visits_fill_nulls_with_0_join_to_timespine
              FROM (
                -- Dedupe the fanout with mf_internal_uuid in the conversion data set
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
                  -- Pass Only Elements: ['__visits_fill_nulls_with_0_join_to_timespine', 'metric_time__day', 'user']
                  SELECT
                    subq_11.metric_time__day
                    , subq_11.user
                    , subq_11.__visits_fill_nulls_with_0_join_to_timespine
                  FROM (
                    -- Pass Only Elements: ['__visits_fill_nulls_with_0_join_to_timespine', 'metric_time__day', 'user']
                    SELECT
                      subq_10.metric_time__day
                      , subq_10.user
                      , subq_10.__visits_fill_nulls_with_0_join_to_timespine
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
                          , EXTRACT(isodow FROM visits_source_src_28000.ds) AS ds__extract_dow
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
                          , EXTRACT(isodow FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                          , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
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
                  -- Add column with generated UUID
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
                    , GEN_RANDOM_UUID() AS mf_internal_uuid
                  FROM (
                    -- Metric Time Dimension 'ds'
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
                        , EXTRACT(isodow FROM buys_source_src_28000.ds) AS ds__extract_dow
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
                        , EXTRACT(isodow FROM buys_source_src_28000.ds) AS buy__ds__extract_dow
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
                      subq_12.metric_time__day > subq_15.metric_time__day - INTERVAL 7 day
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
