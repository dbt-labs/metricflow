-- Compute Metrics via Expressions
SELECT
  subq_12.metric_time__day
  , CAST(subq_12.buys AS DOUBLE) / CAST(NULLIF(subq_12.visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_3.metric_time__day, subq_11.metric_time__day) AS metric_time__day
    , MAX(subq_3.visits) AS visits
    , MAX(subq_11.buys) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      subq_2.metric_time__day
      , SUM(subq_2.visits) AS visits
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_1.metric_time__day
        , subq_1.visits
      FROM (
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['visits', 'metric_time__day']
        SELECT
          subq_0.ds__day AS metric_time__day
          , subq_0.visits
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
        ) subq_0
      ) subq_1
      WHERE metric_time__day = '2020-01-01'
    ) subq_2
    GROUP BY
      subq_2.metric_time__day
  ) subq_3
  FULL OUTER JOIN (
    -- Aggregate Measures
    SELECT
      subq_10.metric_time__day
      , SUM(subq_10.buys) AS buys
    FROM (
      -- Find conversions for user within the range of 7 day
      -- Pass Only Elements: ['buys', 'metric_time__day']
      SELECT
        subq_9.metric_time__day
        , subq_9.buys
      FROM (
        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
        SELECT DISTINCT
          FIRST_VALUE(subq_5.visits) OVER (
            PARTITION BY
              subq_8.user
              , subq_8.ds__day
              , subq_8.mf_internal_uuid
            ORDER BY subq_5.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS visits
          , FIRST_VALUE(subq_5.ds__day) OVER (
            PARTITION BY
              subq_8.user
              , subq_8.ds__day
              , subq_8.mf_internal_uuid
            ORDER BY subq_5.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS ds__day
          , FIRST_VALUE(subq_5.metric_time__day) OVER (
            PARTITION BY
              subq_8.user
              , subq_8.ds__day
              , subq_8.mf_internal_uuid
            ORDER BY subq_5.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS metric_time__day
          , FIRST_VALUE(subq_5.user) OVER (
            PARTITION BY
              subq_8.user
              , subq_8.ds__day
              , subq_8.mf_internal_uuid
            ORDER BY subq_5.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , subq_8.mf_internal_uuid AS mf_internal_uuid
          , subq_8.buys AS buys
        FROM (
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['visits', 'ds__day', 'metric_time__day', 'user']
          SELECT
            subq_4.ds__day
            , subq_4.ds__day AS metric_time__day
            , subq_4.user
            , subq_4.visits
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
        INNER JOIN (
          -- Add column with generated UUID
          SELECT
            subq_7.ds__day
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
            , subq_7.buy__ds__day
            , subq_7.buy__ds__week
            , subq_7.buy__ds__month
            , subq_7.buy__ds__quarter
            , subq_7.buy__ds__year
            , subq_7.buy__ds__extract_year
            , subq_7.buy__ds__extract_quarter
            , subq_7.buy__ds__extract_month
            , subq_7.buy__ds__extract_day
            , subq_7.buy__ds__extract_dow
            , subq_7.buy__ds__extract_doy
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
            , subq_7.session_id
            , subq_7.buy__user
            , subq_7.buy__session_id
            , subq_7.buys
            , subq_7.buyers
            , GEN_RANDOM_UUID() AS mf_internal_uuid
          FROM (
            -- Metric Time Dimension 'ds'
            SELECT
              subq_6.ds__day
              , subq_6.ds__week
              , subq_6.ds__month
              , subq_6.ds__quarter
              , subq_6.ds__year
              , subq_6.ds__extract_year
              , subq_6.ds__extract_quarter
              , subq_6.ds__extract_month
              , subq_6.ds__extract_day
              , subq_6.ds__extract_dow
              , subq_6.ds__extract_doy
              , subq_6.buy__ds__day
              , subq_6.buy__ds__week
              , subq_6.buy__ds__month
              , subq_6.buy__ds__quarter
              , subq_6.buy__ds__year
              , subq_6.buy__ds__extract_year
              , subq_6.buy__ds__extract_quarter
              , subq_6.buy__ds__extract_month
              , subq_6.buy__ds__extract_day
              , subq_6.buy__ds__extract_dow
              , subq_6.buy__ds__extract_doy
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
              , subq_6.user
              , subq_6.session_id
              , subq_6.buy__user
              , subq_6.buy__session_id
              , subq_6.buys
              , subq_6.buyers
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
            ) subq_6
          ) subq_7
        ) subq_8
        ON
          (
            subq_5.user = subq_8.user
          ) AND (
            (
              subq_5.ds__day <= subq_8.ds__day
            ) AND (
              subq_5.ds__day > subq_8.ds__day - INTERVAL 7 day
            )
          )
      ) subq_9
    ) subq_10
    GROUP BY
      subq_10.metric_time__day
  ) subq_11
  ON
    subq_3.metric_time__day = subq_11.metric_time__day
  GROUP BY
    COALESCE(subq_3.metric_time__day, subq_11.metric_time__day)
) subq_12
