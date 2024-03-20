-- Compute Metrics via Expressions
SELECT
  subq_15.visit__referrer_id
  , CAST(subq_15.buys AS DOUBLE) / CAST(NULLIF(subq_15.visits, 0) AS DOUBLE) AS visit_buy_conversion_rate
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_3.visit__referrer_id, subq_14.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_3.visits) AS visits
    , MAX(subq_14.buys) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      subq_2.visit__referrer_id
      , SUM(subq_2.visits) AS visits
    FROM (
      -- Pass Only Elements:
      --   ['visits', 'visit__referrer_id']
      SELECT
        subq_1.visit__referrer_id
        , subq_1.visits
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
          , subq_0.visit__user
          , subq_0.referrer_id
          , subq_0.visit__referrer_id
          , subq_0.visits
          , subq_0.visitors
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          SELECT
            1 AS visits
            , visits_source_src_10011.user_id AS visitors
            , DATE_TRUNC('day', visits_source_src_10011.ds) AS ds__day
            , DATE_TRUNC('week', visits_source_src_10011.ds) AS ds__week
            , DATE_TRUNC('month', visits_source_src_10011.ds) AS ds__month
            , DATE_TRUNC('quarter', visits_source_src_10011.ds) AS ds__quarter
            , DATE_TRUNC('year', visits_source_src_10011.ds) AS ds__year
            , EXTRACT(year FROM visits_source_src_10011.ds) AS ds__extract_year
            , EXTRACT(quarter FROM visits_source_src_10011.ds) AS ds__extract_quarter
            , EXTRACT(month FROM visits_source_src_10011.ds) AS ds__extract_month
            , EXTRACT(day FROM visits_source_src_10011.ds) AS ds__extract_day
            , EXTRACT(isodow FROM visits_source_src_10011.ds) AS ds__extract_dow
            , EXTRACT(doy FROM visits_source_src_10011.ds) AS ds__extract_doy
            , visits_source_src_10011.referrer_id
            , DATE_TRUNC('day', visits_source_src_10011.ds) AS visit__ds__day
            , DATE_TRUNC('week', visits_source_src_10011.ds) AS visit__ds__week
            , DATE_TRUNC('month', visits_source_src_10011.ds) AS visit__ds__month
            , DATE_TRUNC('quarter', visits_source_src_10011.ds) AS visit__ds__quarter
            , DATE_TRUNC('year', visits_source_src_10011.ds) AS visit__ds__year
            , EXTRACT(year FROM visits_source_src_10011.ds) AS visit__ds__extract_year
            , EXTRACT(quarter FROM visits_source_src_10011.ds) AS visit__ds__extract_quarter
            , EXTRACT(month FROM visits_source_src_10011.ds) AS visit__ds__extract_month
            , EXTRACT(day FROM visits_source_src_10011.ds) AS visit__ds__extract_day
            , EXTRACT(isodow FROM visits_source_src_10011.ds) AS visit__ds__extract_dow
            , EXTRACT(doy FROM visits_source_src_10011.ds) AS visit__ds__extract_doy
            , visits_source_src_10011.referrer_id AS visit__referrer_id
            , visits_source_src_10011.user_id AS user
            , visits_source_src_10011.user_id AS visit__user
          FROM ***************************.fct_visits visits_source_src_10011
        ) subq_0
      ) subq_1
    ) subq_2
    GROUP BY
      subq_2.visit__referrer_id
  ) subq_3
  FULL OUTER JOIN (
    -- Aggregate Measures
    SELECT
      subq_13.visit__referrer_id
      , SUM(subq_13.buys) AS buys
    FROM (
      -- Pass Only Elements:
      --   ['buys', 'visit__referrer_id']
      SELECT
        subq_12.visit__referrer_id
        , subq_12.buys
      FROM (
        -- Find conversions for EntitySpec(element_name='user', entity_links=()) within the range of count=7 granularity=TimeGranularity.DAY
        SELECT
          subq_11.visit__referrer_id
          , subq_11.buys
          , subq_11.visits
        FROM (
          -- Dedupe the fanout on (MetadataSpec(element_name='mf_internal_uuid'),) in the conversion data set
          SELECT DISTINCT
            first_value(subq_6.visits) OVER (PARTITION BY subq_10.user, subq_10.ds__day ORDER BY subq_6.ds__day DESC) AS visits
            , first_value(subq_6.visit__referrer_id) OVER (PARTITION BY subq_10.user, subq_10.ds__day ORDER BY subq_6.ds__day DESC) AS visit__referrer_id
            , subq_10.mf_internal_uuid AS mf_internal_uuid
            , subq_10.buys AS buys
          FROM (
            -- Pass Only Elements:
            --   ['visits', 'visit__referrer_id']
            SELECT
              subq_5.visit__referrer_id
              , subq_5.visits
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
                , subq_4.visit__user
                , subq_4.referrer_id
                , subq_4.visit__referrer_id
                , subq_4.visits
                , subq_4.visitors
              FROM (
                -- Read Elements From Semantic Model 'visits_source'
                SELECT
                  1 AS visits
                  , visits_source_src_10011.user_id AS visitors
                  , DATE_TRUNC('day', visits_source_src_10011.ds) AS ds__day
                  , DATE_TRUNC('week', visits_source_src_10011.ds) AS ds__week
                  , DATE_TRUNC('month', visits_source_src_10011.ds) AS ds__month
                  , DATE_TRUNC('quarter', visits_source_src_10011.ds) AS ds__quarter
                  , DATE_TRUNC('year', visits_source_src_10011.ds) AS ds__year
                  , EXTRACT(year FROM visits_source_src_10011.ds) AS ds__extract_year
                  , EXTRACT(quarter FROM visits_source_src_10011.ds) AS ds__extract_quarter
                  , EXTRACT(month FROM visits_source_src_10011.ds) AS ds__extract_month
                  , EXTRACT(day FROM visits_source_src_10011.ds) AS ds__extract_day
                  , EXTRACT(isodow FROM visits_source_src_10011.ds) AS ds__extract_dow
                  , EXTRACT(doy FROM visits_source_src_10011.ds) AS ds__extract_doy
                  , visits_source_src_10011.referrer_id
                  , DATE_TRUNC('day', visits_source_src_10011.ds) AS visit__ds__day
                  , DATE_TRUNC('week', visits_source_src_10011.ds) AS visit__ds__week
                  , DATE_TRUNC('month', visits_source_src_10011.ds) AS visit__ds__month
                  , DATE_TRUNC('quarter', visits_source_src_10011.ds) AS visit__ds__quarter
                  , DATE_TRUNC('year', visits_source_src_10011.ds) AS visit__ds__year
                  , EXTRACT(year FROM visits_source_src_10011.ds) AS visit__ds__extract_year
                  , EXTRACT(quarter FROM visits_source_src_10011.ds) AS visit__ds__extract_quarter
                  , EXTRACT(month FROM visits_source_src_10011.ds) AS visit__ds__extract_month
                  , EXTRACT(day FROM visits_source_src_10011.ds) AS visit__ds__extract_day
                  , EXTRACT(isodow FROM visits_source_src_10011.ds) AS visit__ds__extract_dow
                  , EXTRACT(doy FROM visits_source_src_10011.ds) AS visit__ds__extract_doy
                  , visits_source_src_10011.referrer_id AS visit__referrer_id
                  , visits_source_src_10011.user_id AS user
                  , visits_source_src_10011.user_id AS visit__user
                FROM ***************************.fct_visits visits_source_src_10011
              ) subq_4
            ) subq_5
          ) subq_6
          INNER JOIN (
            -- Pass Only Elements:
            --   ['buys', 'mf_internal_uuid']
            SELECT
              subq_9.buys
              , subq_9.mf_internal_uuid
            FROM (
              -- Add column with generated UUID
              SELECT
                subq_8.ds__day
                , subq_8.ds__week
                , subq_8.ds__month
                , subq_8.ds__quarter
                , subq_8.ds__year
                , subq_8.ds__extract_year
                , subq_8.ds__extract_quarter
                , subq_8.ds__extract_month
                , subq_8.ds__extract_day
                , subq_8.ds__extract_dow
                , subq_8.ds__extract_doy
                , subq_8.buy__ds__day
                , subq_8.buy__ds__week
                , subq_8.buy__ds__month
                , subq_8.buy__ds__quarter
                , subq_8.buy__ds__year
                , subq_8.buy__ds__extract_year
                , subq_8.buy__ds__extract_quarter
                , subq_8.buy__ds__extract_month
                , subq_8.buy__ds__extract_day
                , subq_8.buy__ds__extract_dow
                , subq_8.buy__ds__extract_doy
                , subq_8.metric_time__day
                , subq_8.metric_time__week
                , subq_8.metric_time__month
                , subq_8.metric_time__quarter
                , subq_8.metric_time__year
                , subq_8.metric_time__extract_year
                , subq_8.metric_time__extract_quarter
                , subq_8.metric_time__extract_month
                , subq_8.metric_time__extract_day
                , subq_8.metric_time__extract_dow
                , subq_8.metric_time__extract_doy
                , subq_8.user
                , subq_8.buy__user
                , subq_8.buys
                , subq_8.buyers
                , GEN_RANDOM_UUID() AS mf_internal_uuid
              FROM (
                -- Metric Time Dimension 'ds'
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
                  , subq_7.user
                  , subq_7.buy__user
                  , subq_7.buys
                  , subq_7.buyers
                FROM (
                  -- Read Elements From Semantic Model 'buys_source'
                  SELECT
                    1 AS buys
                    , buys_source_src_10002.user_id AS buyers
                    , DATE_TRUNC('day', buys_source_src_10002.ds) AS ds__day
                    , DATE_TRUNC('week', buys_source_src_10002.ds) AS ds__week
                    , DATE_TRUNC('month', buys_source_src_10002.ds) AS ds__month
                    , DATE_TRUNC('quarter', buys_source_src_10002.ds) AS ds__quarter
                    , DATE_TRUNC('year', buys_source_src_10002.ds) AS ds__year
                    , EXTRACT(year FROM buys_source_src_10002.ds) AS ds__extract_year
                    , EXTRACT(quarter FROM buys_source_src_10002.ds) AS ds__extract_quarter
                    , EXTRACT(month FROM buys_source_src_10002.ds) AS ds__extract_month
                    , EXTRACT(day FROM buys_source_src_10002.ds) AS ds__extract_day
                    , EXTRACT(isodow FROM buys_source_src_10002.ds) AS ds__extract_dow
                    , EXTRACT(doy FROM buys_source_src_10002.ds) AS ds__extract_doy
                    , DATE_TRUNC('day', buys_source_src_10002.ds) AS buy__ds__day
                    , DATE_TRUNC('week', buys_source_src_10002.ds) AS buy__ds__week
                    , DATE_TRUNC('month', buys_source_src_10002.ds) AS buy__ds__month
                    , DATE_TRUNC('quarter', buys_source_src_10002.ds) AS buy__ds__quarter
                    , DATE_TRUNC('year', buys_source_src_10002.ds) AS buy__ds__year
                    , EXTRACT(year FROM buys_source_src_10002.ds) AS buy__ds__extract_year
                    , EXTRACT(quarter FROM buys_source_src_10002.ds) AS buy__ds__extract_quarter
                    , EXTRACT(month FROM buys_source_src_10002.ds) AS buy__ds__extract_month
                    , EXTRACT(day FROM buys_source_src_10002.ds) AS buy__ds__extract_day
                    , EXTRACT(isodow FROM buys_source_src_10002.ds) AS buy__ds__extract_dow
                    , EXTRACT(doy FROM buys_source_src_10002.ds) AS buy__ds__extract_doy
                    , buys_source_src_10002.user_id AS user
                    , buys_source_src_10002.user_id AS buy__user
                  FROM ***************************.fct_buys buys_source_src_10002
                ) subq_7
              ) subq_8
            ) subq_9
          ) subq_10
          ON
            (
              subq_6.user = subq_10.user
            ) AND (
              (
                subq_6.ds__day <= subq_10.ds__day
              ) AND (
                subq_6.ds__day > subq_10.ds__day - INTERVAL 7 day
              )
            )
        ) subq_11
      ) subq_12
    ) subq_13
    GROUP BY
      subq_13.visit__referrer_id
  ) subq_14
  ON
    subq_3.visit__referrer_id = subq_14.visit__referrer_id
  GROUP BY
    COALESCE(subq_3.visit__referrer_id, subq_14.visit__referrer_id)
) subq_15
