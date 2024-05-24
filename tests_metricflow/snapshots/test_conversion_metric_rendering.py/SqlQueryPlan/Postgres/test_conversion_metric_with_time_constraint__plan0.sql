-- Compute Metrics via Expressions
SELECT
  subq_21.visit__referrer_id
  , CAST(subq_21.buys AS DOUBLE PRECISION) / CAST(NULLIF(subq_21.visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_9.visit__referrer_id, subq_20.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_9.visits) AS visits
    , MAX(subq_20.buys) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      subq_8.visit__referrer_id
      , SUM(subq_8.visits) AS visits
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_7.visit__referrer_id
        , subq_7.visits
      FROM (
        -- Pass Only Elements: ['visits', 'visit__referrer_id']
        SELECT
          subq_6.visit__referrer_id
          , subq_6.visits
        FROM (
          -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
          SELECT
            subq_5.ds__day
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
            , subq_5.visit__ds__day
            , subq_5.visit__ds__week
            , subq_5.visit__ds__month
            , subq_5.visit__ds__quarter
            , subq_5.visit__ds__year
            , subq_5.visit__ds__extract_year
            , subq_5.visit__ds__extract_quarter
            , subq_5.visit__ds__extract_month
            , subq_5.visit__ds__extract_day
            , subq_5.visit__ds__extract_dow
            , subq_5.visit__ds__extract_doy
            , subq_5.metric_time__day
            , subq_5.metric_time__week
            , subq_5.metric_time__month
            , subq_5.metric_time__quarter
            , subq_5.metric_time__year
            , subq_5.metric_time__extract_year
            , subq_5.metric_time__extract_quarter
            , subq_5.metric_time__extract_month
            , subq_5.metric_time__extract_day
            , subq_5.metric_time__extract_dow
            , subq_5.metric_time__extract_doy
            , subq_5.user
            , subq_5.session
            , subq_5.visit__user
            , subq_5.visit__session
            , subq_5.referrer_id
            , subq_5.visit__referrer_id
            , subq_5.visits
            , subq_5.visitors
          FROM (
            -- Metric Time Dimension 'ds'
            SELECT
              subq_4.ds__day
              , subq_4.ds__week
              , subq_4.ds__month
              , subq_4.ds__quarter
              , subq_4.ds__year
              , subq_4.ds__extract_year
              , subq_4.ds__extract_quarter
              , subq_4.ds__extract_month
              , subq_4.ds__extract_day
              , subq_4.ds__extract_dow
              , subq_4.ds__extract_doy
              , subq_4.visit__ds__day
              , subq_4.visit__ds__week
              , subq_4.visit__ds__month
              , subq_4.visit__ds__quarter
              , subq_4.visit__ds__year
              , subq_4.visit__ds__extract_year
              , subq_4.visit__ds__extract_quarter
              , subq_4.visit__ds__extract_month
              , subq_4.visit__ds__extract_day
              , subq_4.visit__ds__extract_dow
              , subq_4.visit__ds__extract_doy
              , subq_4.ds__day AS metric_time__day
              , subq_4.ds__week AS metric_time__week
              , subq_4.ds__month AS metric_time__month
              , subq_4.ds__quarter AS metric_time__quarter
              , subq_4.ds__year AS metric_time__year
              , subq_4.ds__extract_year AS metric_time__extract_year
              , subq_4.ds__extract_quarter AS metric_time__extract_quarter
              , subq_4.ds__extract_month AS metric_time__extract_month
              , subq_4.ds__extract_day AS metric_time__extract_day
              , subq_4.ds__extract_dow AS metric_time__extract_dow
              , subq_4.ds__extract_doy AS metric_time__extract_doy
              , subq_4.user
              , subq_4.session
              , subq_4.visit__user
              , subq_4.visit__session
              , subq_4.referrer_id
              , subq_4.visit__referrer_id
              , subq_4.visits
              , subq_4.visitors
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
            ) subq_4
          ) subq_5
          WHERE subq_5.metric_time__day BETWEEN '2020-01-01' AND '2020-01-02'
        ) subq_6
      ) subq_7
      WHERE visit__referrer_id = 'ref_id_01'
    ) subq_8
    GROUP BY
      subq_8.visit__referrer_id
  ) subq_9
  FULL OUTER JOIN (
    -- Aggregate Measures
    SELECT
      subq_19.visit__referrer_id
      , SUM(subq_19.buys) AS buys
    FROM (
      -- Pass Only Elements: ['buys', 'visit__referrer_id']
      SELECT
        subq_18.visit__referrer_id
        , subq_18.buys
      FROM (
        -- Find conversions for user within the range of INF
        SELECT
          subq_17.ds__day
          , subq_17.user
          , subq_17.visit__referrer_id
          , subq_17.buys
          , subq_17.visits
        FROM (
          -- Dedupe the fanout with mf_internal_uuid in the conversion data set
          SELECT DISTINCT
            first_value(subq_13.visits) OVER (
              PARTITION BY
                subq_16.user
                , subq_16.ds__day
                , subq_16.mf_internal_uuid
              ORDER BY subq_13.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visits
            , first_value(subq_13.visit__referrer_id) OVER (
              PARTITION BY
                subq_16.user
                , subq_16.ds__day
                , subq_16.mf_internal_uuid
              ORDER BY subq_13.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visit__referrer_id
            , first_value(subq_13.ds__day) OVER (
              PARTITION BY
                subq_16.user
                , subq_16.ds__day
                , subq_16.mf_internal_uuid
              ORDER BY subq_13.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS ds__day
            , first_value(subq_13.user) OVER (
              PARTITION BY
                subq_16.user
                , subq_16.ds__day
                , subq_16.mf_internal_uuid
              ORDER BY subq_13.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS user
            , subq_16.mf_internal_uuid AS mf_internal_uuid
            , subq_16.buys AS buys
          FROM (
            -- Pass Only Elements: ['visits', 'visit__referrer_id', 'ds__day', 'user']
            SELECT
              subq_12.ds__day
              , subq_12.user
              , subq_12.visit__referrer_id
              , subq_12.visits
            FROM (
              -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
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
                , subq_11.visit__ds__day
                , subq_11.visit__ds__week
                , subq_11.visit__ds__month
                , subq_11.visit__ds__quarter
                , subq_11.visit__ds__year
                , subq_11.visit__ds__extract_year
                , subq_11.visit__ds__extract_quarter
                , subq_11.visit__ds__extract_month
                , subq_11.visit__ds__extract_day
                , subq_11.visit__ds__extract_dow
                , subq_11.visit__ds__extract_doy
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
                , subq_11.session
                , subq_11.visit__user
                , subq_11.visit__session
                , subq_11.referrer_id
                , subq_11.visit__referrer_id
                , subq_11.visits
                , subq_11.visitors
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
                  , subq_10.visit__ds__day
                  , subq_10.visit__ds__week
                  , subq_10.visit__ds__month
                  , subq_10.visit__ds__quarter
                  , subq_10.visit__ds__year
                  , subq_10.visit__ds__extract_year
                  , subq_10.visit__ds__extract_quarter
                  , subq_10.visit__ds__extract_month
                  , subq_10.visit__ds__extract_day
                  , subq_10.visit__ds__extract_dow
                  , subq_10.visit__ds__extract_doy
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
                  , subq_10.session
                  , subq_10.visit__user
                  , subq_10.visit__session
                  , subq_10.referrer_id
                  , subq_10.visit__referrer_id
                  , subq_10.visits
                  , subq_10.visitors
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
                ) subq_10
              ) subq_11
              WHERE subq_11.metric_time__day BETWEEN '2020-01-01' AND '2020-01-02'
            ) subq_12
          ) subq_13
          INNER JOIN (
            -- Add column with generated UUID
            SELECT
              subq_15.ds__day
              , subq_15.ds__week
              , subq_15.ds__month
              , subq_15.ds__quarter
              , subq_15.ds__year
              , subq_15.ds__extract_year
              , subq_15.ds__extract_quarter
              , subq_15.ds__extract_month
              , subq_15.ds__extract_day
              , subq_15.ds__extract_dow
              , subq_15.ds__extract_doy
              , subq_15.buy__ds__day
              , subq_15.buy__ds__week
              , subq_15.buy__ds__month
              , subq_15.buy__ds__quarter
              , subq_15.buy__ds__year
              , subq_15.buy__ds__extract_year
              , subq_15.buy__ds__extract_quarter
              , subq_15.buy__ds__extract_month
              , subq_15.buy__ds__extract_day
              , subq_15.buy__ds__extract_dow
              , subq_15.buy__ds__extract_doy
              , subq_15.metric_time__day
              , subq_15.metric_time__week
              , subq_15.metric_time__month
              , subq_15.metric_time__quarter
              , subq_15.metric_time__year
              , subq_15.metric_time__extract_year
              , subq_15.metric_time__extract_quarter
              , subq_15.metric_time__extract_month
              , subq_15.metric_time__extract_day
              , subq_15.metric_time__extract_dow
              , subq_15.metric_time__extract_doy
              , subq_15.user
              , subq_15.session_id
              , subq_15.buy__user
              , subq_15.buy__session_id
              , subq_15.buys
              , subq_15.buyers
              , GEN_RANDOM_UUID() AS mf_internal_uuid
            FROM (
              -- Metric Time Dimension 'ds'
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
                , subq_14.ds__day AS metric_time__day
                , subq_14.ds__week AS metric_time__week
                , subq_14.ds__month AS metric_time__month
                , subq_14.ds__quarter AS metric_time__quarter
                , subq_14.ds__year AS metric_time__year
                , subq_14.ds__extract_year AS metric_time__extract_year
                , subq_14.ds__extract_quarter AS metric_time__extract_quarter
                , subq_14.ds__extract_month AS metric_time__extract_month
                , subq_14.ds__extract_day AS metric_time__extract_day
                , subq_14.ds__extract_dow AS metric_time__extract_dow
                , subq_14.ds__extract_doy AS metric_time__extract_doy
                , subq_14.user
                , subq_14.session_id
                , subq_14.buy__user
                , subq_14.buy__session_id
                , subq_14.buys
                , subq_14.buyers
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
                  , EXTRACT(isodow FROM buys_source_src_28000.ds) AS ds__extract_dow
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
                  , EXTRACT(isodow FROM buys_source_src_28000.ds) AS buy__ds__extract_dow
                  , EXTRACT(doy FROM buys_source_src_28000.ds) AS buy__ds__extract_doy
                  , buys_source_src_28000.user_id AS user
                  , buys_source_src_28000.session_id
                  , buys_source_src_28000.user_id AS buy__user
                  , buys_source_src_28000.session_id AS buy__session_id
                FROM ***************************.fct_buys buys_source_src_28000
              ) subq_14
            ) subq_15
          ) subq_16
          ON
            (
              subq_13.user = subq_16.user
            ) AND (
              (subq_13.ds__day <= subq_16.ds__day)
            )
        ) subq_17
      ) subq_18
    ) subq_19
    GROUP BY
      subq_19.visit__referrer_id
  ) subq_20
  ON
    subq_9.visit__referrer_id = subq_20.visit__referrer_id
  GROUP BY
    COALESCE(subq_9.visit__referrer_id, subq_20.visit__referrer_id)
) subq_21
