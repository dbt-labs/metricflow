-- Compute Metrics via Expressions
SELECT
  subq_17.metric_time__day
  , CAST(subq_17.buys AS DOUBLE) / CAST(NULLIF(subq_17.visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days_fill_nulls_with_0
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_5.metric_time__day, subq_16.metric_time__day) AS metric_time__day
    , COALESCE(MAX(subq_5.visits), 0) AS visits
    , COALESCE(MAX(subq_16.buys), 0) AS buys
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_3.metric_time__day AS metric_time__day
      , subq_2.visits AS visits
    FROM (
      -- Time Spine
      SELECT
        subq_4.ds AS metric_time__day
      FROM ***************************.mf_time_spine subq_4
    ) subq_3
    LEFT OUTER JOIN (
      -- Aggregate Measures
      SELECT
        subq_1.metric_time__day
        , SUM(subq_1.visits) AS visits
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
      GROUP BY
        subq_1.metric_time__day
    ) subq_2
    ON
      subq_3.metric_time__day = subq_2.metric_time__day
  ) subq_5
  FULL OUTER JOIN (
    -- Join to Time Spine Dataset
    SELECT
      subq_14.metric_time__day AS metric_time__day
      , subq_13.buys AS buys
    FROM (
      -- Time Spine
      SELECT
        subq_15.ds AS metric_time__day
      FROM ***************************.mf_time_spine subq_15
    ) subq_14
    LEFT OUTER JOIN (
      -- Aggregate Measures
      SELECT
        subq_12.metric_time__day
        , SUM(subq_12.buys) AS buys
      FROM (
        -- Find conversions for user within the range of 7 day
        -- Pass Only Elements: ['buys', 'metric_time__day']
        SELECT
          subq_11.metric_time__day
          , subq_11.buys
        FROM (
          -- Dedupe the fanout with mf_internal_uuid in the conversion data set
          SELECT DISTINCT
            FIRST_VALUE(subq_7.visits) OVER (
              PARTITION BY
                subq_10.user
                , subq_10.ds__day
                , subq_10.mf_internal_uuid
              ORDER BY subq_7.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visits
            , FIRST_VALUE(subq_7.ds__day) OVER (
              PARTITION BY
                subq_10.user
                , subq_10.ds__day
                , subq_10.mf_internal_uuid
              ORDER BY subq_7.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS ds__day
            , FIRST_VALUE(subq_7.metric_time__day) OVER (
              PARTITION BY
                subq_10.user
                , subq_10.ds__day
                , subq_10.mf_internal_uuid
              ORDER BY subq_7.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS metric_time__day
            , FIRST_VALUE(subq_7.user) OVER (
              PARTITION BY
                subq_10.user
                , subq_10.ds__day
                , subq_10.mf_internal_uuid
              ORDER BY subq_7.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS user
            , subq_10.mf_internal_uuid AS mf_internal_uuid
            , subq_10.buys AS buys
          FROM (
            -- Metric Time Dimension 'ds'
            -- Pass Only Elements: ['visits', 'ds__day', 'metric_time__day', 'user']
            SELECT
              subq_6.ds__day
              , subq_6.ds__day AS metric_time__day
              , subq_6.user
              , subq_6.visits
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
            ) subq_6
          ) subq_7
          INNER JOIN (
            -- Add column with generated UUID
            SELECT
              subq_9.ds__day
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
              , subq_9.buy__ds__day
              , subq_9.buy__ds__week
              , subq_9.buy__ds__month
              , subq_9.buy__ds__quarter
              , subq_9.buy__ds__year
              , subq_9.buy__ds__extract_year
              , subq_9.buy__ds__extract_quarter
              , subq_9.buy__ds__extract_month
              , subq_9.buy__ds__extract_day
              , subq_9.buy__ds__extract_dow
              , subq_9.buy__ds__extract_doy
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
              , subq_9.session_id
              , subq_9.buy__user
              , subq_9.buy__session_id
              , subq_9.buys
              , subq_9.buyers
              , GEN_RANDOM_UUID() AS mf_internal_uuid
            FROM (
              -- Metric Time Dimension 'ds'
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
                , subq_8.ds__day AS metric_time__day
                , subq_8.ds__week AS metric_time__week
                , subq_8.ds__month AS metric_time__month
                , subq_8.ds__quarter AS metric_time__quarter
                , subq_8.ds__year AS metric_time__year
                , subq_8.ds__extract_year AS metric_time__extract_year
                , subq_8.ds__extract_quarter AS metric_time__extract_quarter
                , subq_8.ds__extract_month AS metric_time__extract_month
                , subq_8.ds__extract_day AS metric_time__extract_day
                , subq_8.ds__extract_dow AS metric_time__extract_dow
                , subq_8.ds__extract_doy AS metric_time__extract_doy
                , subq_8.user
                , subq_8.session_id
                , subq_8.buy__user
                , subq_8.buy__session_id
                , subq_8.buys
                , subq_8.buyers
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
              ) subq_8
            ) subq_9
          ) subq_10
          ON
            (
              subq_7.user = subq_10.user
            ) AND (
              (
                subq_7.ds__day <= subq_10.ds__day
              ) AND (
                subq_7.ds__day > subq_10.ds__day - INTERVAL 7 day
              )
            )
        ) subq_11
      ) subq_12
      GROUP BY
        subq_12.metric_time__day
    ) subq_13
    ON
      subq_14.metric_time__day = subq_13.metric_time__day
  ) subq_16
  ON
    subq_5.metric_time__day = subq_16.metric_time__day
  GROUP BY
    COALESCE(subq_5.metric_time__day, subq_16.metric_time__day)
) subq_17
