-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , user__home_state_latest
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_37.metric_time__day, subq_54.metric_time__day) AS metric_time__day
    , COALESCE(subq_37.user__home_state_latest, subq_54.user__home_state_latest) AS user__home_state_latest
    , MAX(subq_37.visits) AS visits
    , MAX(subq_54.buys) AS buys
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['visits', 'user__home_state_latest', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , user__home_state_latest
      , SUM(visits) AS visits
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['visits', 'user__home_state_latest', 'visit__referrer_id', 'metric_time__day']
      SELECT
        subq_30.metric_time__day AS metric_time__day
        , subq_30.visit__referrer_id AS visit__referrer_id
        , users_latest_src_28000.home_state_latest AS user__home_state_latest
        , subq_30.visits AS visits
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day', 'user']
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , referrer_id AS visit__referrer_id
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) subq_30
      LEFT OUTER JOIN
        ***************************.dim_users_latest users_latest_src_28000
      ON
        subq_30.user = users_latest_src_28000.user_id
    ) subq_34
    WHERE visit__referrer_id = '123456'
    GROUP BY
      metric_time__day
      , user__home_state_latest
  ) subq_37
  FULL OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements: ['buys', 'user__home_state_latest', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      subq_47.metric_time__day AS metric_time__day
      , users_latest_src_28000.home_state_latest AS user__home_state_latest
      , SUM(subq_47.buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_43.visits) OVER (
          PARTITION BY
            subq_46.user
            , subq_46.ds__day
            , subq_46.mf_internal_uuid
          ORDER BY subq_43.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_43.visit__referrer_id) OVER (
          PARTITION BY
            subq_46.user
            , subq_46.ds__day
            , subq_46.mf_internal_uuid
          ORDER BY subq_43.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(subq_43.user__home_state_latest) OVER (
          PARTITION BY
            subq_46.user
            , subq_46.ds__day
            , subq_46.mf_internal_uuid
          ORDER BY subq_43.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user__home_state_latest
        , FIRST_VALUE(subq_43.ds__day) OVER (
          PARTITION BY
            subq_46.user
            , subq_46.ds__day
            , subq_46.mf_internal_uuid
          ORDER BY subq_43.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__day
        , FIRST_VALUE(subq_43.metric_time__day) OVER (
          PARTITION BY
            subq_46.user
            , subq_46.ds__day
            , subq_46.mf_internal_uuid
          ORDER BY subq_43.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_43.user) OVER (
          PARTITION BY
            subq_46.user
            , subq_46.ds__day
            , subq_46.mf_internal_uuid
          ORDER BY subq_43.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_46.mf_internal_uuid AS mf_internal_uuid
        , subq_46.buys AS buys
      FROM (
        -- Join Standard Outputs
        -- Pass Only Elements: ['visits', 'visit__referrer_id', 'user__home_state_latest', 'ds__day', 'metric_time__day', 'user']
        SELECT
          subq_39.ds__day AS ds__day
          , subq_39.metric_time__day AS metric_time__day
          , subq_39.user AS user
          , subq_39.visit__referrer_id AS visit__referrer_id
          , users_latest_src_28000.home_state_latest AS user__home_state_latest
          , subq_39.visits AS visits
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATE_TRUNC('day', ds) AS ds__day
            , DATE_TRUNC('day', ds) AS metric_time__day
            , user_id AS user
            , referrer_id AS visit__referrer_id
            , 1 AS visits
          FROM ***************************.fct_visits visits_source_src_28000
        ) subq_39
        LEFT OUTER JOIN
          ***************************.dim_users_latest users_latest_src_28000
        ON
          subq_39.user = users_latest_src_28000.user_id
      ) subq_43
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS ds__day
          , user_id AS user
          , 1 AS buys
          , UUID_STRING() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_46
      ON
        (
          subq_43.user = subq_46.user
        ) AND (
          (
            subq_43.ds__day <= subq_46.ds__day
          ) AND (
            subq_43.ds__day > DATEADD(day, -7, subq_46.ds__day)
          )
        )
    ) subq_47
    LEFT OUTER JOIN
      ***************************.dim_users_latest users_latest_src_28000
    ON
      subq_47.user = users_latest_src_28000.user_id
    GROUP BY
      subq_47.metric_time__day
      , users_latest_src_28000.home_state_latest
  ) subq_54
  ON
    (
      subq_37.user__home_state_latest = subq_54.user__home_state_latest
    ) AND (
      subq_37.metric_time__day = subq_54.metric_time__day
    )
  GROUP BY
    COALESCE(subq_37.metric_time__day, subq_54.metric_time__day)
    , COALESCE(subq_37.user__home_state_latest, subq_54.user__home_state_latest)
) subq_55
