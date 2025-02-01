test_name: test_conversion_metric_with_custom_granularity_filter
test_filename: test_custom_granularity.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  subq_17.metric_time__alien_day
  , CAST(subq_17.buys AS FLOAT64) / CAST(NULLIF(subq_17.visits, 0) AS FLOAT64) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_5.metric_time__alien_day, subq_16.metric_time__alien_day) AS metric_time__alien_day
    , MAX(subq_5.visits) AS visits
    , MAX(subq_16.buys) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      subq_4.metric_time__alien_day
      , SUM(subq_4.visits) AS visits
    FROM (
      -- Pass Only Elements: ['visits', 'metric_time__alien_day']
      SELECT
        subq_3.metric_time__alien_day
        , subq_3.visits
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_2.metric_time__alien_day
          , subq_2.ds__day
          , subq_2.ds__week
          , subq_2.ds__month
          , subq_2.ds__quarter
          , subq_2.ds__year
          , subq_2.ds__extract_year
          , subq_2.ds__extract_quarter
          , subq_2.ds__extract_month
          , subq_2.ds__extract_day
          , subq_2.ds__extract_dow
          , subq_2.ds__extract_doy
          , subq_2.visit__ds__day
          , subq_2.visit__ds__week
          , subq_2.visit__ds__month
          , subq_2.visit__ds__quarter
          , subq_2.visit__ds__year
          , subq_2.visit__ds__extract_year
          , subq_2.visit__ds__extract_quarter
          , subq_2.visit__ds__extract_month
          , subq_2.visit__ds__extract_day
          , subq_2.visit__ds__extract_dow
          , subq_2.visit__ds__extract_doy
          , subq_2.metric_time__day
          , subq_2.metric_time__week
          , subq_2.metric_time__month
          , subq_2.metric_time__quarter
          , subq_2.metric_time__year
          , subq_2.metric_time__extract_year
          , subq_2.metric_time__extract_quarter
          , subq_2.metric_time__extract_month
          , subq_2.metric_time__extract_day
          , subq_2.metric_time__extract_dow
          , subq_2.metric_time__extract_doy
          , subq_2.user
          , subq_2.session
          , subq_2.visit__user
          , subq_2.visit__session
          , subq_2.referrer_id
          , subq_2.visit__referrer_id
          , subq_2.visits
          , subq_2.visitors
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
            , subq_0.visits AS visits
            , subq_0.visitors AS visitors
            , subq_1.alien_day AS metric_time__alien_day
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
          ) subq_0
          LEFT OUTER JOIN
            ***************************.mf_time_spine subq_1
          ON
            subq_0.ds__day = subq_1.ds
        ) subq_2
        WHERE metric_time__alien_day = '2020-01-01'
      ) subq_3
    ) subq_4
    GROUP BY
      metric_time__alien_day
  ) subq_5
  FULL OUTER JOIN (
    -- Aggregate Measures
    SELECT
      subq_15.metric_time__alien_day
      , SUM(subq_15.buys) AS buys
    FROM (
      -- Pass Only Elements: ['buys', 'metric_time__alien_day']
      SELECT
        subq_14.metric_time__alien_day
        , subq_14.buys
      FROM (
        -- Find conversions for user within the range of 7 day
        SELECT
          subq_13.metric_time__alien_day
          , subq_13.metric_time__day
          , subq_13.user
          , subq_13.buys
          , subq_13.visits
        FROM (
          -- Dedupe the fanout with mf_internal_uuid in the conversion data set
          SELECT DISTINCT
            FIRST_VALUE(subq_9.visits) OVER (
              PARTITION BY
                subq_12.user
                , subq_12.metric_time__day
                , subq_12.mf_internal_uuid
              ORDER BY subq_9.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visits
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
            , subq_12.buys AS buys
          FROM (
            -- Pass Only Elements: ['visits', 'metric_time__day', 'metric_time__alien_day', 'user']
            SELECT
              subq_8.metric_time__alien_day
              , subq_8.metric_time__day
              , subq_8.user
              , subq_8.visits
            FROM (
              -- Constrain Output with WHERE
              SELECT
                subq_7.metric_time__alien_day
                , subq_7.ds__day
                , subq_7.ds__week
                , subq_7.ds__month
                , subq_7.ds__quarter
                , subq_7.ds__year
                , subq_7.ds__extract_year
                , subq_7.ds__extract_quarter
                , subq_7.ds__extract_month
                , subq_7.ds__extract_day
                , subq_7.ds__extract_dow
                , subq_7.ds__extract_doy
                , subq_7.visit__ds__day
                , subq_7.visit__ds__week
                , subq_7.visit__ds__month
                , subq_7.visit__ds__quarter
                , subq_7.visit__ds__year
                , subq_7.visit__ds__extract_year
                , subq_7.visit__ds__extract_quarter
                , subq_7.visit__ds__extract_month
                , subq_7.visit__ds__extract_day
                , subq_7.visit__ds__extract_dow
                , subq_7.visit__ds__extract_doy
                , subq_7.metric_time__day
                , subq_7.metric_time__week
                , subq_7.metric_time__month
                , subq_7.metric_time__quarter
                , subq_7.metric_time__year
                , subq_7.metric_time__extract_year
                , subq_7.metric_time__extract_quarter
                , subq_7.metric_time__extract_month
                , subq_7.metric_time__extract_day
                , subq_7.metric_time__extract_dow
                , subq_7.metric_time__extract_doy
                , subq_7.user
                , subq_7.session
                , subq_7.visit__user
                , subq_7.visit__session
                , subq_7.referrer_id
                , subq_7.visit__referrer_id
                , subq_7.visits
                , subq_7.visitors
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
                  , subq_0.visits AS visits
                  , subq_0.visitors AS visitors
                  , subq_6.alien_day AS metric_time__alien_day
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
                ) subq_0
                LEFT OUTER JOIN
                  ***************************.mf_time_spine subq_6
                ON
                  subq_0.ds__day = subq_6.ds
              ) subq_7
              WHERE metric_time__alien_day = '2020-01-01'
            ) subq_8
          ) subq_9
          INNER JOIN (
            -- Add column with generated UUID
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
              , subq_11.buys
              , subq_11.buyers
              , GENERATE_UUID() AS mf_internal_uuid
            FROM (
              -- Metric Time Dimension 'ds'
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
                , subq_10.buys
                , subq_10.buyers
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
                subq_9.metric_time__day > DATE_SUB(CAST(subq_12.metric_time__day AS DATETIME), INTERVAL 7 day)
              )
            )
        ) subq_13
      ) subq_14
    ) subq_15
    GROUP BY
      metric_time__alien_day
  ) subq_16
  ON
    subq_5.metric_time__alien_day = subq_16.metric_time__alien_day
  GROUP BY
    metric_time__alien_day
) subq_17
