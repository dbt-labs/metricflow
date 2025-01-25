test_name: test_conversion_metric_join_to_timespine_and_fill_nulls_with_0
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric that joins to time spine and fills nulls with 0.
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_19.metric_time__day
  , CAST(nr_subq_19.buys AS FLOAT64) / CAST(NULLIF(nr_subq_19.visits, 0) AS FLOAT64) AS visit_buy_conversion_rate_7days_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_6.metric_time__day, nr_subq_18.metric_time__day) AS metric_time__day
    , COALESCE(MAX(nr_subq_6.visits), 0) AS visits
    , COALESCE(MAX(nr_subq_18.buys), 0) AS buys
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      nr_subq_5.metric_time__day AS metric_time__day
      , nr_subq_2.visits AS visits
    FROM (
      -- Pass Only Elements: ['metric_time__day',]
      SELECT
        nr_subq_4.metric_time__day
      FROM (
        -- Change Column Aliases
        SELECT
          nr_subq_3.ds__day AS metric_time__day
          , nr_subq_3.ds__week
          , nr_subq_3.ds__month
          , nr_subq_3.ds__quarter
          , nr_subq_3.ds__year
          , nr_subq_3.ds__extract_year
          , nr_subq_3.ds__extract_quarter
          , nr_subq_3.ds__extract_month
          , nr_subq_3.ds__extract_day
          , nr_subq_3.ds__extract_dow
          , nr_subq_3.ds__extract_doy
          , nr_subq_3.ds__martian_day
        FROM (
          -- Read From Time Spine 'mf_time_spine'
          SELECT
            time_spine_src_28006.ds AS ds__day
            , DATETIME_TRUNC(time_spine_src_28006.ds, isoweek) AS ds__week
            , DATETIME_TRUNC(time_spine_src_28006.ds, month) AS ds__month
            , DATETIME_TRUNC(time_spine_src_28006.ds, quarter) AS ds__quarter
            , DATETIME_TRUNC(time_spine_src_28006.ds, year) AS ds__year
            , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
            , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
            , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
            , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
            , IF(EXTRACT(dayofweek FROM time_spine_src_28006.ds) = 1, 7, EXTRACT(dayofweek FROM time_spine_src_28006.ds) - 1) AS ds__extract_dow
            , EXTRACT(dayofyear FROM time_spine_src_28006.ds) AS ds__extract_doy
            , time_spine_src_28006.martian_day AS ds__martian_day
          FROM ***************************.mf_time_spine time_spine_src_28006
        ) nr_subq_3
      ) nr_subq_4
    ) nr_subq_5
    LEFT OUTER JOIN (
      -- Aggregate Measures
      SELECT
        nr_subq_1.metric_time__day
        , SUM(nr_subq_1.visits) AS visits
      FROM (
        -- Pass Only Elements: ['visits', 'metric_time__day']
        SELECT
          nr_subq_0.metric_time__day
          , nr_subq_0.visits
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            nr_subq_28012.ds__day
            , nr_subq_28012.ds__week
            , nr_subq_28012.ds__month
            , nr_subq_28012.ds__quarter
            , nr_subq_28012.ds__year
            , nr_subq_28012.ds__extract_year
            , nr_subq_28012.ds__extract_quarter
            , nr_subq_28012.ds__extract_month
            , nr_subq_28012.ds__extract_day
            , nr_subq_28012.ds__extract_dow
            , nr_subq_28012.ds__extract_doy
            , nr_subq_28012.visit__ds__day
            , nr_subq_28012.visit__ds__week
            , nr_subq_28012.visit__ds__month
            , nr_subq_28012.visit__ds__quarter
            , nr_subq_28012.visit__ds__year
            , nr_subq_28012.visit__ds__extract_year
            , nr_subq_28012.visit__ds__extract_quarter
            , nr_subq_28012.visit__ds__extract_month
            , nr_subq_28012.visit__ds__extract_day
            , nr_subq_28012.visit__ds__extract_dow
            , nr_subq_28012.visit__ds__extract_doy
            , nr_subq_28012.ds__day AS metric_time__day
            , nr_subq_28012.ds__week AS metric_time__week
            , nr_subq_28012.ds__month AS metric_time__month
            , nr_subq_28012.ds__quarter AS metric_time__quarter
            , nr_subq_28012.ds__year AS metric_time__year
            , nr_subq_28012.ds__extract_year AS metric_time__extract_year
            , nr_subq_28012.ds__extract_quarter AS metric_time__extract_quarter
            , nr_subq_28012.ds__extract_month AS metric_time__extract_month
            , nr_subq_28012.ds__extract_day AS metric_time__extract_day
            , nr_subq_28012.ds__extract_dow AS metric_time__extract_dow
            , nr_subq_28012.ds__extract_doy AS metric_time__extract_doy
            , nr_subq_28012.user
            , nr_subq_28012.session
            , nr_subq_28012.visit__user
            , nr_subq_28012.visit__session
            , nr_subq_28012.referrer_id
            , nr_subq_28012.visit__referrer_id
            , nr_subq_28012.visits
            , nr_subq_28012.visitors
          FROM (
            -- Read Elements From Semantic Model 'visits_source'
            SELECT
              1 AS visits
              , visits_source_src_28000.user_id AS visitors
              , DATETIME_TRUNC(visits_source_src_28000.ds, day) AS ds__day
              , DATETIME_TRUNC(visits_source_src_28000.ds, isoweek) AS ds__week
              , DATETIME_TRUNC(visits_source_src_28000.ds, month) AS ds__month
              , DATETIME_TRUNC(visits_source_src_28000.ds, quarter) AS ds__quarter
              , DATETIME_TRUNC(visits_source_src_28000.ds, year) AS ds__year
              , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
              , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
              , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
              , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
              , IF(EXTRACT(dayofweek FROM visits_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM visits_source_src_28000.ds) - 1) AS ds__extract_dow
              , EXTRACT(dayofyear FROM visits_source_src_28000.ds) AS ds__extract_doy
              , visits_source_src_28000.referrer_id
              , DATETIME_TRUNC(visits_source_src_28000.ds, day) AS visit__ds__day
              , DATETIME_TRUNC(visits_source_src_28000.ds, isoweek) AS visit__ds__week
              , DATETIME_TRUNC(visits_source_src_28000.ds, month) AS visit__ds__month
              , DATETIME_TRUNC(visits_source_src_28000.ds, quarter) AS visit__ds__quarter
              , DATETIME_TRUNC(visits_source_src_28000.ds, year) AS visit__ds__year
              , EXTRACT(year FROM visits_source_src_28000.ds) AS visit__ds__extract_year
              , EXTRACT(quarter FROM visits_source_src_28000.ds) AS visit__ds__extract_quarter
              , EXTRACT(month FROM visits_source_src_28000.ds) AS visit__ds__extract_month
              , EXTRACT(day FROM visits_source_src_28000.ds) AS visit__ds__extract_day
              , IF(EXTRACT(dayofweek FROM visits_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM visits_source_src_28000.ds) - 1) AS visit__ds__extract_dow
              , EXTRACT(dayofyear FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
              , visits_source_src_28000.referrer_id AS visit__referrer_id
              , visits_source_src_28000.user_id AS user
              , visits_source_src_28000.session_id AS session
              , visits_source_src_28000.user_id AS visit__user
              , visits_source_src_28000.session_id AS visit__session
            FROM ***************************.fct_visits visits_source_src_28000
          ) nr_subq_28012
        ) nr_subq_0
      ) nr_subq_1
      GROUP BY
        metric_time__day
    ) nr_subq_2
    ON
      nr_subq_5.metric_time__day = nr_subq_2.metric_time__day
  ) nr_subq_6
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    SELECT
      nr_subq_17.metric_time__day AS metric_time__day
      , nr_subq_14.buys AS buys
    FROM (
      -- Pass Only Elements: ['metric_time__day',]
      SELECT
        nr_subq_16.metric_time__day
      FROM (
        -- Change Column Aliases
        SELECT
          nr_subq_15.ds__day AS metric_time__day
          , nr_subq_15.ds__week
          , nr_subq_15.ds__month
          , nr_subq_15.ds__quarter
          , nr_subq_15.ds__year
          , nr_subq_15.ds__extract_year
          , nr_subq_15.ds__extract_quarter
          , nr_subq_15.ds__extract_month
          , nr_subq_15.ds__extract_day
          , nr_subq_15.ds__extract_dow
          , nr_subq_15.ds__extract_doy
          , nr_subq_15.ds__martian_day
        FROM (
          -- Read From Time Spine 'mf_time_spine'
          SELECT
            time_spine_src_28006.ds AS ds__day
            , DATETIME_TRUNC(time_spine_src_28006.ds, isoweek) AS ds__week
            , DATETIME_TRUNC(time_spine_src_28006.ds, month) AS ds__month
            , DATETIME_TRUNC(time_spine_src_28006.ds, quarter) AS ds__quarter
            , DATETIME_TRUNC(time_spine_src_28006.ds, year) AS ds__year
            , EXTRACT(year FROM time_spine_src_28006.ds) AS ds__extract_year
            , EXTRACT(quarter FROM time_spine_src_28006.ds) AS ds__extract_quarter
            , EXTRACT(month FROM time_spine_src_28006.ds) AS ds__extract_month
            , EXTRACT(day FROM time_spine_src_28006.ds) AS ds__extract_day
            , IF(EXTRACT(dayofweek FROM time_spine_src_28006.ds) = 1, 7, EXTRACT(dayofweek FROM time_spine_src_28006.ds) - 1) AS ds__extract_dow
            , EXTRACT(dayofyear FROM time_spine_src_28006.ds) AS ds__extract_doy
            , time_spine_src_28006.martian_day AS ds__martian_day
          FROM ***************************.mf_time_spine time_spine_src_28006
        ) nr_subq_15
      ) nr_subq_16
    ) nr_subq_17
    LEFT OUTER JOIN (
      -- Aggregate Measures
      SELECT
        nr_subq_13.metric_time__day
        , SUM(nr_subq_13.buys) AS buys
      FROM (
        -- Pass Only Elements: ['buys', 'metric_time__day']
        SELECT
          nr_subq_12.metric_time__day
          , nr_subq_12.buys
        FROM (
          -- Find conversions for user within the range of 7 day
          SELECT
            nr_subq_11.metric_time__day
            , nr_subq_11.user
            , nr_subq_11.buys
            , nr_subq_11.visits
          FROM (
            -- Dedupe the fanout with mf_internal_uuid in the conversion data set
            SELECT DISTINCT
              FIRST_VALUE(nr_subq_8.visits) OVER (
                PARTITION BY
                  nr_subq_10.user
                  , nr_subq_10.metric_time__day
                  , nr_subq_10.mf_internal_uuid
                ORDER BY nr_subq_8.metric_time__day DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
              ) AS visits
              , FIRST_VALUE(nr_subq_8.metric_time__day) OVER (
                PARTITION BY
                  nr_subq_10.user
                  , nr_subq_10.metric_time__day
                  , nr_subq_10.mf_internal_uuid
                ORDER BY nr_subq_8.metric_time__day DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
              ) AS metric_time__day
              , FIRST_VALUE(nr_subq_8.user) OVER (
                PARTITION BY
                  nr_subq_10.user
                  , nr_subq_10.metric_time__day
                  , nr_subq_10.mf_internal_uuid
                ORDER BY nr_subq_8.metric_time__day DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
              ) AS user
              , nr_subq_10.mf_internal_uuid AS mf_internal_uuid
              , nr_subq_10.buys AS buys
            FROM (
              -- Pass Only Elements: ['visits', 'metric_time__day', 'user']
              SELECT
                nr_subq_7.metric_time__day
                , nr_subq_7.user
                , nr_subq_7.visits
              FROM (
                -- Metric Time Dimension 'ds'
                SELECT
                  nr_subq_28012.ds__day
                  , nr_subq_28012.ds__week
                  , nr_subq_28012.ds__month
                  , nr_subq_28012.ds__quarter
                  , nr_subq_28012.ds__year
                  , nr_subq_28012.ds__extract_year
                  , nr_subq_28012.ds__extract_quarter
                  , nr_subq_28012.ds__extract_month
                  , nr_subq_28012.ds__extract_day
                  , nr_subq_28012.ds__extract_dow
                  , nr_subq_28012.ds__extract_doy
                  , nr_subq_28012.visit__ds__day
                  , nr_subq_28012.visit__ds__week
                  , nr_subq_28012.visit__ds__month
                  , nr_subq_28012.visit__ds__quarter
                  , nr_subq_28012.visit__ds__year
                  , nr_subq_28012.visit__ds__extract_year
                  , nr_subq_28012.visit__ds__extract_quarter
                  , nr_subq_28012.visit__ds__extract_month
                  , nr_subq_28012.visit__ds__extract_day
                  , nr_subq_28012.visit__ds__extract_dow
                  , nr_subq_28012.visit__ds__extract_doy
                  , nr_subq_28012.ds__day AS metric_time__day
                  , nr_subq_28012.ds__week AS metric_time__week
                  , nr_subq_28012.ds__month AS metric_time__month
                  , nr_subq_28012.ds__quarter AS metric_time__quarter
                  , nr_subq_28012.ds__year AS metric_time__year
                  , nr_subq_28012.ds__extract_year AS metric_time__extract_year
                  , nr_subq_28012.ds__extract_quarter AS metric_time__extract_quarter
                  , nr_subq_28012.ds__extract_month AS metric_time__extract_month
                  , nr_subq_28012.ds__extract_day AS metric_time__extract_day
                  , nr_subq_28012.ds__extract_dow AS metric_time__extract_dow
                  , nr_subq_28012.ds__extract_doy AS metric_time__extract_doy
                  , nr_subq_28012.user
                  , nr_subq_28012.session
                  , nr_subq_28012.visit__user
                  , nr_subq_28012.visit__session
                  , nr_subq_28012.referrer_id
                  , nr_subq_28012.visit__referrer_id
                  , nr_subq_28012.visits
                  , nr_subq_28012.visitors
                FROM (
                  -- Read Elements From Semantic Model 'visits_source'
                  SELECT
                    1 AS visits
                    , visits_source_src_28000.user_id AS visitors
                    , DATETIME_TRUNC(visits_source_src_28000.ds, day) AS ds__day
                    , DATETIME_TRUNC(visits_source_src_28000.ds, isoweek) AS ds__week
                    , DATETIME_TRUNC(visits_source_src_28000.ds, month) AS ds__month
                    , DATETIME_TRUNC(visits_source_src_28000.ds, quarter) AS ds__quarter
                    , DATETIME_TRUNC(visits_source_src_28000.ds, year) AS ds__year
                    , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
                    , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
                    , IF(EXTRACT(dayofweek FROM visits_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM visits_source_src_28000.ds) - 1) AS ds__extract_dow
                    , EXTRACT(dayofyear FROM visits_source_src_28000.ds) AS ds__extract_doy
                    , visits_source_src_28000.referrer_id
                    , DATETIME_TRUNC(visits_source_src_28000.ds, day) AS visit__ds__day
                    , DATETIME_TRUNC(visits_source_src_28000.ds, isoweek) AS visit__ds__week
                    , DATETIME_TRUNC(visits_source_src_28000.ds, month) AS visit__ds__month
                    , DATETIME_TRUNC(visits_source_src_28000.ds, quarter) AS visit__ds__quarter
                    , DATETIME_TRUNC(visits_source_src_28000.ds, year) AS visit__ds__year
                    , EXTRACT(year FROM visits_source_src_28000.ds) AS visit__ds__extract_year
                    , EXTRACT(quarter FROM visits_source_src_28000.ds) AS visit__ds__extract_quarter
                    , EXTRACT(month FROM visits_source_src_28000.ds) AS visit__ds__extract_month
                    , EXTRACT(day FROM visits_source_src_28000.ds) AS visit__ds__extract_day
                    , IF(EXTRACT(dayofweek FROM visits_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM visits_source_src_28000.ds) - 1) AS visit__ds__extract_dow
                    , EXTRACT(dayofyear FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                    , visits_source_src_28000.referrer_id AS visit__referrer_id
                    , visits_source_src_28000.user_id AS user
                    , visits_source_src_28000.session_id AS session
                    , visits_source_src_28000.user_id AS visit__user
                    , visits_source_src_28000.session_id AS visit__session
                  FROM ***************************.fct_visits visits_source_src_28000
                ) nr_subq_28012
              ) nr_subq_7
            ) nr_subq_8
            INNER JOIN (
              -- Add column with generated UUID
              SELECT
                nr_subq_9.ds__day
                , nr_subq_9.ds__week
                , nr_subq_9.ds__month
                , nr_subq_9.ds__quarter
                , nr_subq_9.ds__year
                , nr_subq_9.ds__extract_year
                , nr_subq_9.ds__extract_quarter
                , nr_subq_9.ds__extract_month
                , nr_subq_9.ds__extract_day
                , nr_subq_9.ds__extract_dow
                , nr_subq_9.ds__extract_doy
                , nr_subq_9.ds_month__month
                , nr_subq_9.ds_month__quarter
                , nr_subq_9.ds_month__year
                , nr_subq_9.ds_month__extract_year
                , nr_subq_9.ds_month__extract_quarter
                , nr_subq_9.ds_month__extract_month
                , nr_subq_9.buy__ds__day
                , nr_subq_9.buy__ds__week
                , nr_subq_9.buy__ds__month
                , nr_subq_9.buy__ds__quarter
                , nr_subq_9.buy__ds__year
                , nr_subq_9.buy__ds__extract_year
                , nr_subq_9.buy__ds__extract_quarter
                , nr_subq_9.buy__ds__extract_month
                , nr_subq_9.buy__ds__extract_day
                , nr_subq_9.buy__ds__extract_dow
                , nr_subq_9.buy__ds__extract_doy
                , nr_subq_9.buy__ds_month__month
                , nr_subq_9.buy__ds_month__quarter
                , nr_subq_9.buy__ds_month__year
                , nr_subq_9.buy__ds_month__extract_year
                , nr_subq_9.buy__ds_month__extract_quarter
                , nr_subq_9.buy__ds_month__extract_month
                , nr_subq_9.metric_time__day
                , nr_subq_9.metric_time__week
                , nr_subq_9.metric_time__month
                , nr_subq_9.metric_time__quarter
                , nr_subq_9.metric_time__year
                , nr_subq_9.metric_time__extract_year
                , nr_subq_9.metric_time__extract_quarter
                , nr_subq_9.metric_time__extract_month
                , nr_subq_9.metric_time__extract_day
                , nr_subq_9.metric_time__extract_dow
                , nr_subq_9.metric_time__extract_doy
                , nr_subq_9.user
                , nr_subq_9.session_id
                , nr_subq_9.buy__user
                , nr_subq_9.buy__session_id
                , nr_subq_9.buys
                , nr_subq_9.buyers
                , GENERATE_UUID() AS mf_internal_uuid
              FROM (
                -- Metric Time Dimension 'ds'
                SELECT
                  nr_subq_28004.ds__day
                  , nr_subq_28004.ds__week
                  , nr_subq_28004.ds__month
                  , nr_subq_28004.ds__quarter
                  , nr_subq_28004.ds__year
                  , nr_subq_28004.ds__extract_year
                  , nr_subq_28004.ds__extract_quarter
                  , nr_subq_28004.ds__extract_month
                  , nr_subq_28004.ds__extract_day
                  , nr_subq_28004.ds__extract_dow
                  , nr_subq_28004.ds__extract_doy
                  , nr_subq_28004.ds_month__month
                  , nr_subq_28004.ds_month__quarter
                  , nr_subq_28004.ds_month__year
                  , nr_subq_28004.ds_month__extract_year
                  , nr_subq_28004.ds_month__extract_quarter
                  , nr_subq_28004.ds_month__extract_month
                  , nr_subq_28004.buy__ds__day
                  , nr_subq_28004.buy__ds__week
                  , nr_subq_28004.buy__ds__month
                  , nr_subq_28004.buy__ds__quarter
                  , nr_subq_28004.buy__ds__year
                  , nr_subq_28004.buy__ds__extract_year
                  , nr_subq_28004.buy__ds__extract_quarter
                  , nr_subq_28004.buy__ds__extract_month
                  , nr_subq_28004.buy__ds__extract_day
                  , nr_subq_28004.buy__ds__extract_dow
                  , nr_subq_28004.buy__ds__extract_doy
                  , nr_subq_28004.buy__ds_month__month
                  , nr_subq_28004.buy__ds_month__quarter
                  , nr_subq_28004.buy__ds_month__year
                  , nr_subq_28004.buy__ds_month__extract_year
                  , nr_subq_28004.buy__ds_month__extract_quarter
                  , nr_subq_28004.buy__ds_month__extract_month
                  , nr_subq_28004.ds__day AS metric_time__day
                  , nr_subq_28004.ds__week AS metric_time__week
                  , nr_subq_28004.ds__month AS metric_time__month
                  , nr_subq_28004.ds__quarter AS metric_time__quarter
                  , nr_subq_28004.ds__year AS metric_time__year
                  , nr_subq_28004.ds__extract_year AS metric_time__extract_year
                  , nr_subq_28004.ds__extract_quarter AS metric_time__extract_quarter
                  , nr_subq_28004.ds__extract_month AS metric_time__extract_month
                  , nr_subq_28004.ds__extract_day AS metric_time__extract_day
                  , nr_subq_28004.ds__extract_dow AS metric_time__extract_dow
                  , nr_subq_28004.ds__extract_doy AS metric_time__extract_doy
                  , nr_subq_28004.user
                  , nr_subq_28004.session_id
                  , nr_subq_28004.buy__user
                  , nr_subq_28004.buy__session_id
                  , nr_subq_28004.buys
                  , nr_subq_28004.buyers
                FROM (
                  -- Read Elements From Semantic Model 'buys_source'
                  SELECT
                    1 AS buys
                    , 1 AS buys_month
                    , buys_source_src_28000.user_id AS buyers
                    , DATETIME_TRUNC(buys_source_src_28000.ds, day) AS ds__day
                    , DATETIME_TRUNC(buys_source_src_28000.ds, isoweek) AS ds__week
                    , DATETIME_TRUNC(buys_source_src_28000.ds, month) AS ds__month
                    , DATETIME_TRUNC(buys_source_src_28000.ds, quarter) AS ds__quarter
                    , DATETIME_TRUNC(buys_source_src_28000.ds, year) AS ds__year
                    , EXTRACT(year FROM buys_source_src_28000.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM buys_source_src_28000.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM buys_source_src_28000.ds) AS ds__extract_month
                    , EXTRACT(day FROM buys_source_src_28000.ds) AS ds__extract_day
                    , IF(EXTRACT(dayofweek FROM buys_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM buys_source_src_28000.ds) - 1) AS ds__extract_dow
                    , EXTRACT(dayofyear FROM buys_source_src_28000.ds) AS ds__extract_doy
                    , DATETIME_TRUNC(buys_source_src_28000.ds_month, month) AS ds_month__month
                    , DATETIME_TRUNC(buys_source_src_28000.ds_month, quarter) AS ds_month__quarter
                    , DATETIME_TRUNC(buys_source_src_28000.ds_month, year) AS ds_month__year
                    , EXTRACT(year FROM buys_source_src_28000.ds_month) AS ds_month__extract_year
                    , EXTRACT(quarter FROM buys_source_src_28000.ds_month) AS ds_month__extract_quarter
                    , EXTRACT(month FROM buys_source_src_28000.ds_month) AS ds_month__extract_month
                    , DATETIME_TRUNC(buys_source_src_28000.ds, day) AS buy__ds__day
                    , DATETIME_TRUNC(buys_source_src_28000.ds, isoweek) AS buy__ds__week
                    , DATETIME_TRUNC(buys_source_src_28000.ds, month) AS buy__ds__month
                    , DATETIME_TRUNC(buys_source_src_28000.ds, quarter) AS buy__ds__quarter
                    , DATETIME_TRUNC(buys_source_src_28000.ds, year) AS buy__ds__year
                    , EXTRACT(year FROM buys_source_src_28000.ds) AS buy__ds__extract_year
                    , EXTRACT(quarter FROM buys_source_src_28000.ds) AS buy__ds__extract_quarter
                    , EXTRACT(month FROM buys_source_src_28000.ds) AS buy__ds__extract_month
                    , EXTRACT(day FROM buys_source_src_28000.ds) AS buy__ds__extract_day
                    , IF(EXTRACT(dayofweek FROM buys_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM buys_source_src_28000.ds) - 1) AS buy__ds__extract_dow
                    , EXTRACT(dayofyear FROM buys_source_src_28000.ds) AS buy__ds__extract_doy
                    , DATETIME_TRUNC(buys_source_src_28000.ds_month, month) AS buy__ds_month__month
                    , DATETIME_TRUNC(buys_source_src_28000.ds_month, quarter) AS buy__ds_month__quarter
                    , DATETIME_TRUNC(buys_source_src_28000.ds_month, year) AS buy__ds_month__year
                    , EXTRACT(year FROM buys_source_src_28000.ds_month) AS buy__ds_month__extract_year
                    , EXTRACT(quarter FROM buys_source_src_28000.ds_month) AS buy__ds_month__extract_quarter
                    , EXTRACT(month FROM buys_source_src_28000.ds_month) AS buy__ds_month__extract_month
                    , buys_source_src_28000.user_id AS user
                    , buys_source_src_28000.session_id
                    , buys_source_src_28000.user_id AS buy__user
                    , buys_source_src_28000.session_id AS buy__session_id
                  FROM ***************************.fct_buys buys_source_src_28000
                ) nr_subq_28004
              ) nr_subq_9
            ) nr_subq_10
            ON
              (
                nr_subq_8.user = nr_subq_10.user
              ) AND (
                (
                  nr_subq_8.metric_time__day <= nr_subq_10.metric_time__day
                ) AND (
                  nr_subq_8.metric_time__day > DATE_SUB(CAST(nr_subq_10.metric_time__day AS DATETIME), INTERVAL 7 day)
                )
              )
          ) nr_subq_11
        ) nr_subq_12
      ) nr_subq_13
      GROUP BY
        metric_time__day
    ) nr_subq_14
    ON
      nr_subq_17.metric_time__day = nr_subq_14.metric_time__day
  ) nr_subq_18
  ON
    nr_subq_6.metric_time__day = nr_subq_18.metric_time__day
  GROUP BY
    metric_time__day
) nr_subq_19
