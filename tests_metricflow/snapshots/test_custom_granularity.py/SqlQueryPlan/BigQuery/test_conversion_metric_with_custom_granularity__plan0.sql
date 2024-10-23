-- Compute Metrics via Expressions
SELECT
  subq_18.metric_time__martian_day
  , CAST(subq_18.buys AS FLOAT64) / CAST(NULLIF(subq_18.visits, 0) AS FLOAT64) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_5.metric_time__martian_day, subq_17.metric_time__martian_day) AS metric_time__martian_day
    , MAX(subq_5.visits) AS visits
    , MAX(subq_17.buys) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      subq_4.metric_time__martian_day
      , SUM(subq_4.visits) AS visits
    FROM (
      -- Pass Only Elements: ['visits', 'metric_time__martian_day']
      SELECT
        subq_3.metric_time__martian_day
        , subq_3.visits
      FROM (
        -- Pass Only Elements: ['visits', 'metric_time__day']
        -- Join to Custom Granularity Dataset
        SELECT
          subq_1.metric_time__day AS metric_time__day
          , subq_1.visits AS visits
          , subq_2.martian_day AS metric_time__martian_day
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
            , subq_0.visits
            , subq_0.visitors
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
        ) subq_1
        LEFT OUTER JOIN
          ***************************.mf_time_spine subq_2
        ON
          subq_1.metric_time__day = subq_2.ds
      ) subq_3
    ) subq_4
    GROUP BY
      metric_time__martian_day
  ) subq_5
  FULL OUTER JOIN (
    -- Aggregate Measures
    SELECT
      subq_16.metric_time__martian_day
      , SUM(subq_16.buys) AS buys
    FROM (
      -- Pass Only Elements: ['buys', 'metric_time__martian_day']
      SELECT
        subq_15.metric_time__martian_day
        , subq_15.buys
      FROM (
        -- Pass Only Elements: ['buys', 'metric_time__martian_day']
        SELECT
          subq_14.metric_time__martian_day
          , subq_14.buys
        FROM (
          -- Find conversions for user within the range of 7 day
          SELECT
            subq_13.metric_time__martian_day
            , subq_13.ds__day
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
                  , subq_12.ds__day
                  , subq_12.mf_internal_uuid
                ORDER BY subq_9.ds__day DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
              ) AS visits
              , FIRST_VALUE(subq_9.metric_time__martian_day) OVER (
                PARTITION BY
                  subq_12.user
                  , subq_12.ds__day
                  , subq_12.mf_internal_uuid
                ORDER BY subq_9.ds__day DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
              ) AS metric_time__martian_day
              , FIRST_VALUE(subq_9.ds__day) OVER (
                PARTITION BY
                  subq_12.user
                  , subq_12.ds__day
                  , subq_12.mf_internal_uuid
                ORDER BY subq_9.ds__day DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
              ) AS ds__day
              , FIRST_VALUE(subq_9.metric_time__day) OVER (
                PARTITION BY
                  subq_12.user
                  , subq_12.ds__day
                  , subq_12.mf_internal_uuid
                ORDER BY subq_9.ds__day DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
              ) AS metric_time__day
              , FIRST_VALUE(subq_9.user) OVER (
                PARTITION BY
                  subq_12.user
                  , subq_12.ds__day
                  , subq_12.mf_internal_uuid
                ORDER BY subq_9.ds__day DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
              ) AS user
              , subq_12.mf_internal_uuid AS mf_internal_uuid
              , subq_12.buys AS buys
            FROM (
              -- Pass Only Elements: ['visits', 'ds__day', 'metric_time__day', 'metric_time__martian_day', 'user']
              SELECT
                subq_8.metric_time__martian_day
                , subq_8.ds__day
                , subq_8.metric_time__day
                , subq_8.user
                , subq_8.visits
              FROM (
                -- Metric Time Dimension 'ds'
                -- Join to Custom Granularity Dataset
                SELECT
                  subq_6.ds__day AS ds__day
                  , subq_6.ds__week AS ds__week
                  , subq_6.ds__month AS ds__month
                  , subq_6.ds__quarter AS ds__quarter
                  , subq_6.ds__year AS ds__year
                  , subq_6.ds__extract_year AS ds__extract_year
                  , subq_6.ds__extract_quarter AS ds__extract_quarter
                  , subq_6.ds__extract_month AS ds__extract_month
                  , subq_6.ds__extract_day AS ds__extract_day
                  , subq_6.ds__extract_dow AS ds__extract_dow
                  , subq_6.ds__extract_doy AS ds__extract_doy
                  , subq_6.visit__ds__day AS visit__ds__day
                  , subq_6.visit__ds__week AS visit__ds__week
                  , subq_6.visit__ds__month AS visit__ds__month
                  , subq_6.visit__ds__quarter AS visit__ds__quarter
                  , subq_6.visit__ds__year AS visit__ds__year
                  , subq_6.visit__ds__extract_year AS visit__ds__extract_year
                  , subq_6.visit__ds__extract_quarter AS visit__ds__extract_quarter
                  , subq_6.visit__ds__extract_month AS visit__ds__extract_month
                  , subq_6.visit__ds__extract_day AS visit__ds__extract_day
                  , subq_6.visit__ds__extract_dow AS visit__ds__extract_dow
                  , subq_6.visit__ds__extract_doy AS visit__ds__extract_doy
                  , subq_6.ds__day AS metric_time__day
                  , subq_6.ds__week AS metric_time__week
                  , subq_6.ds__month AS metric_time__month
                  , subq_6.ds__quarter AS metric_time__quarter
                  , subq_6.ds__year AS metric_time__year
                  , subq_6.ds__extract_year AS metric_time__extract_year
                  , subq_6.ds__extract_quarter AS metric_time__extract_quarter
                  , subq_6.ds__extract_month AS metric_time__extract_month
                  , subq_6.ds__extract_day AS metric_time__extract_day
                  , subq_6.ds__extract_dow AS metric_time__extract_dow
                  , subq_6.ds__extract_doy AS metric_time__extract_doy
                  , subq_6.user AS user
                  , subq_6.session AS session
                  , subq_6.visit__user AS visit__user
                  , subq_6.visit__session AS visit__session
                  , subq_6.referrer_id AS referrer_id
                  , subq_6.visit__referrer_id AS visit__referrer_id
                  , subq_6.visits AS visits
                  , subq_6.visitors AS visitors
                  , subq_7.martian_day AS metric_time__martian_day
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
                ) subq_6
                LEFT OUTER JOIN
                  ***************************.mf_time_spine subq_7
                ON
                  subq_6.ds__day = subq_7.ds
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
                  subq_9.ds__day <= subq_12.ds__day
                ) AND (
                  subq_9.ds__day > DATE_SUB(CAST(subq_12.ds__day AS DATETIME), INTERVAL 7 day)
                )
              )
          ) subq_13
        ) subq_14
      ) subq_15
    ) subq_16
    GROUP BY
      metric_time__martian_day
  ) subq_17
  ON
    subq_5.metric_time__martian_day = subq_17.metric_time__martian_day
  GROUP BY
    metric_time__martian_day
) subq_18
