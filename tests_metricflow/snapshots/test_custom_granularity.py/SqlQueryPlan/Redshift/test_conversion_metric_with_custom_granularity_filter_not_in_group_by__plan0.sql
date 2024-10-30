-- Compute Metrics via Expressions
SELECT
  CAST(subq_19.buys AS DOUBLE PRECISION) / CAST(NULLIF(subq_19.visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    MAX(subq_6.visits) AS visits
    , MAX(subq_18.buys) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      SUM(subq_5.visits) AS visits
    FROM (
      -- Pass Only Elements: ['visits',]
      SELECT
        subq_4.visits
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_3.metric_time__martian_day
          , subq_3.metric_time__day
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
                , DATE_TRUNC('day', visits_source_src_28000.ds) AS ds__day
                , DATE_TRUNC('week', visits_source_src_28000.ds) AS ds__week
                , DATE_TRUNC('month', visits_source_src_28000.ds) AS ds__month
                , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS ds__quarter
                , DATE_TRUNC('year', visits_source_src_28000.ds) AS ds__year
                , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
                , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
                , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
                , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
                , CASE WHEN EXTRACT(dow FROM visits_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM visits_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM visits_source_src_28000.ds) END AS ds__extract_dow
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
                , CASE WHEN EXTRACT(dow FROM visits_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM visits_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM visits_source_src_28000.ds) END AS visit__ds__extract_dow
                , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
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
        WHERE metric_time__martian_day = '2020-01-01'
      ) subq_4
    ) subq_5
  ) subq_6
  CROSS JOIN (
    -- Aggregate Measures
    SELECT
      SUM(subq_17.buys) AS buys
    FROM (
      -- Pass Only Elements: ['buys',]
      SELECT
        subq_16.buys
      FROM (
        -- Find conversions for user within the range of 7 day
        SELECT
          subq_15.metric_time__martian_day
          , subq_15.metric_time__day
          , subq_15.user
          , subq_15.buys
          , subq_15.visits
        FROM (
          -- Dedupe the fanout with mf_internal_uuid in the conversion data set
          SELECT DISTINCT
            FIRST_VALUE(subq_11.visits) OVER (
              PARTITION BY
                subq_14.user
                , subq_14.metric_time__day
                , subq_14.mf_internal_uuid
              ORDER BY subq_11.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visits
            , FIRST_VALUE(subq_11.metric_time__martian_day) OVER (
              PARTITION BY
                subq_14.user
                , subq_14.metric_time__day
                , subq_14.mf_internal_uuid
              ORDER BY subq_11.metric_time__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS metric_time__martian_day
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
            , subq_14.buys AS buys
          FROM (
            -- Pass Only Elements: ['visits', 'metric_time__day', 'metric_time__martian_day', 'user']
            SELECT
              subq_10.metric_time__martian_day
              , subq_10.metric_time__day
              , subq_10.user
              , subq_10.visits
            FROM (
              -- Constrain Output with WHERE
              SELECT
                subq_9.metric_time__martian_day
                , subq_9.ds__day
                , subq_9.ds__week
                , subq_9.ds__month
                , subq_9.ds__quarter
                , subq_9.ds__year
                , subq_9.ds__extract_year
                , subq_9.ds__extract_quarter
                , subq_9.ds__extract_month
                , subq_9.ds__extract_day
                , subq_9.ds__extract_dow
                , subq_9.ds__extract_doy
                , subq_9.visit__ds__day
                , subq_9.visit__ds__week
                , subq_9.visit__ds__month
                , subq_9.visit__ds__quarter
                , subq_9.visit__ds__year
                , subq_9.visit__ds__extract_year
                , subq_9.visit__ds__extract_quarter
                , subq_9.visit__ds__extract_month
                , subq_9.visit__ds__extract_day
                , subq_9.visit__ds__extract_dow
                , subq_9.visit__ds__extract_doy
                , subq_9.metric_time__day
                , subq_9.metric_time__week
                , subq_9.metric_time__month
                , subq_9.metric_time__quarter
                , subq_9.metric_time__year
                , subq_9.metric_time__extract_year
                , subq_9.metric_time__extract_quarter
                , subq_9.metric_time__extract_month
                , subq_9.metric_time__extract_day
                , subq_9.metric_time__extract_dow
                , subq_9.metric_time__extract_doy
                , subq_9.user
                , subq_9.session
                , subq_9.visit__user
                , subq_9.visit__session
                , subq_9.referrer_id
                , subq_9.visit__referrer_id
                , subq_9.visits
                , subq_9.visitors
              FROM (
                -- Metric Time Dimension 'ds'
                -- Join to Custom Granularity Dataset
                SELECT
                  subq_7.ds__day AS ds__day
                  , subq_7.ds__week AS ds__week
                  , subq_7.ds__month AS ds__month
                  , subq_7.ds__quarter AS ds__quarter
                  , subq_7.ds__year AS ds__year
                  , subq_7.ds__extract_year AS ds__extract_year
                  , subq_7.ds__extract_quarter AS ds__extract_quarter
                  , subq_7.ds__extract_month AS ds__extract_month
                  , subq_7.ds__extract_day AS ds__extract_day
                  , subq_7.ds__extract_dow AS ds__extract_dow
                  , subq_7.ds__extract_doy AS ds__extract_doy
                  , subq_7.visit__ds__day AS visit__ds__day
                  , subq_7.visit__ds__week AS visit__ds__week
                  , subq_7.visit__ds__month AS visit__ds__month
                  , subq_7.visit__ds__quarter AS visit__ds__quarter
                  , subq_7.visit__ds__year AS visit__ds__year
                  , subq_7.visit__ds__extract_year AS visit__ds__extract_year
                  , subq_7.visit__ds__extract_quarter AS visit__ds__extract_quarter
                  , subq_7.visit__ds__extract_month AS visit__ds__extract_month
                  , subq_7.visit__ds__extract_day AS visit__ds__extract_day
                  , subq_7.visit__ds__extract_dow AS visit__ds__extract_dow
                  , subq_7.visit__ds__extract_doy AS visit__ds__extract_doy
                  , subq_7.ds__day AS metric_time__day
                  , subq_7.ds__week AS metric_time__week
                  , subq_7.ds__month AS metric_time__month
                  , subq_7.ds__quarter AS metric_time__quarter
                  , subq_7.ds__year AS metric_time__year
                  , subq_7.ds__extract_year AS metric_time__extract_year
                  , subq_7.ds__extract_quarter AS metric_time__extract_quarter
                  , subq_7.ds__extract_month AS metric_time__extract_month
                  , subq_7.ds__extract_day AS metric_time__extract_day
                  , subq_7.ds__extract_dow AS metric_time__extract_dow
                  , subq_7.ds__extract_doy AS metric_time__extract_doy
                  , subq_7.user AS user
                  , subq_7.session AS session
                  , subq_7.visit__user AS visit__user
                  , subq_7.visit__session AS visit__session
                  , subq_7.referrer_id AS referrer_id
                  , subq_7.visit__referrer_id AS visit__referrer_id
                  , subq_7.visits AS visits
                  , subq_7.visitors AS visitors
                  , subq_8.martian_day AS metric_time__martian_day
                FROM (
                  -- Read Elements From Semantic Model 'visits_source'
                  SELECT
                    1 AS visits
                    , visits_source_src_28000.user_id AS visitors
                    , DATE_TRUNC('day', visits_source_src_28000.ds) AS ds__day
                    , DATE_TRUNC('week', visits_source_src_28000.ds) AS ds__week
                    , DATE_TRUNC('month', visits_source_src_28000.ds) AS ds__month
                    , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS ds__quarter
                    , DATE_TRUNC('year', visits_source_src_28000.ds) AS ds__year
                    , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
                    , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
                    , CASE WHEN EXTRACT(dow FROM visits_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM visits_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM visits_source_src_28000.ds) END AS ds__extract_dow
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
                    , CASE WHEN EXTRACT(dow FROM visits_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM visits_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM visits_source_src_28000.ds) END AS visit__ds__extract_dow
                    , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                    , visits_source_src_28000.referrer_id AS visit__referrer_id
                    , visits_source_src_28000.user_id AS user
                    , visits_source_src_28000.session_id AS session
                    , visits_source_src_28000.user_id AS visit__user
                    , visits_source_src_28000.session_id AS visit__session
                  FROM ***************************.fct_visits visits_source_src_28000
                ) subq_7
                LEFT OUTER JOIN
                  ***************************.mf_time_spine subq_8
                ON
                  subq_7.ds__day = subq_8.ds
              ) subq_9
              WHERE metric_time__martian_day = '2020-01-01'
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
              , subq_13.buys
              , subq_13.buyers
              , CONCAT(CAST(RANDOM()*100000000 AS INT)::VARCHAR,CAST(RANDOM()*100000000 AS INT)::VARCHAR) AS mf_internal_uuid
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
                , subq_12.buys
                , subq_12.buyers
              FROM (
                -- Read Elements From Semantic Model 'buys_source'
                SELECT
                  1 AS buys
                  , buys_source_src_28000.user_id AS buyers
                  , DATE_TRUNC('day', buys_source_src_28000.ds) AS ds__day
                  , DATE_TRUNC('week', buys_source_src_28000.ds) AS ds__week
                  , DATE_TRUNC('month', buys_source_src_28000.ds) AS ds__month
                  , DATE_TRUNC('quarter', buys_source_src_28000.ds) AS ds__quarter
                  , DATE_TRUNC('year', buys_source_src_28000.ds) AS ds__year
                  , EXTRACT(year FROM buys_source_src_28000.ds) AS ds__extract_year
                  , EXTRACT(quarter FROM buys_source_src_28000.ds) AS ds__extract_quarter
                  , EXTRACT(month FROM buys_source_src_28000.ds) AS ds__extract_month
                  , EXTRACT(day FROM buys_source_src_28000.ds) AS ds__extract_day
                  , CASE WHEN EXTRACT(dow FROM buys_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM buys_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM buys_source_src_28000.ds) END AS ds__extract_dow
                  , EXTRACT(doy FROM buys_source_src_28000.ds) AS ds__extract_doy
                  , DATE_TRUNC('day', buys_source_src_28000.ds) AS buy__ds__day
                  , DATE_TRUNC('week', buys_source_src_28000.ds) AS buy__ds__week
                  , DATE_TRUNC('month', buys_source_src_28000.ds) AS buy__ds__month
                  , DATE_TRUNC('quarter', buys_source_src_28000.ds) AS buy__ds__quarter
                  , DATE_TRUNC('year', buys_source_src_28000.ds) AS buy__ds__year
                  , EXTRACT(year FROM buys_source_src_28000.ds) AS buy__ds__extract_year
                  , EXTRACT(quarter FROM buys_source_src_28000.ds) AS buy__ds__extract_quarter
                  , EXTRACT(month FROM buys_source_src_28000.ds) AS buy__ds__extract_month
                  , EXTRACT(day FROM buys_source_src_28000.ds) AS buy__ds__extract_day
                  , CASE WHEN EXTRACT(dow FROM buys_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM buys_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM buys_source_src_28000.ds) END AS buy__ds__extract_dow
                  , EXTRACT(doy FROM buys_source_src_28000.ds) AS buy__ds__extract_doy
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
                subq_11.metric_time__day > DATEADD(day, -7, subq_14.metric_time__day)
              )
            )
        ) subq_15
      ) subq_16
    ) subq_17
  ) subq_18
) subq_19
