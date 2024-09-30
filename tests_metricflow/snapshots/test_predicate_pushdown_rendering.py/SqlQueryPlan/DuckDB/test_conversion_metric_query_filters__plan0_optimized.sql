-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , user__home_state_latest
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_24.metric_time__day, subq_36.metric_time__day) AS metric_time__day
    , COALESCE(subq_24.user__home_state_latest, subq_36.user__home_state_latest) AS user__home_state_latest
    , MAX(subq_24.visits) AS visits
    , MAX(subq_36.buys) AS buys
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
        subq_20.metric_time__day AS metric_time__day
        , subq_20.visit__referrer_id AS visit__referrer_id
        , subq_20.visits AS visits
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
      ) subq_20
      LEFT OUTER JOIN
        ***************************.dim_users_latest users_latest_src_28000
      ON
        subq_20.user = users_latest_src_28000.user_id
    ) subq_22
    WHERE visit__referrer_id = '123456'
    GROUP BY
      metric_time__day
      , user__home_state_latest
  ) subq_24
  FULL OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements: ['buys', 'user__home_state_latest', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      subq_32.metric_time__day AS metric_time__day
      , subq_35.user__home_state_latest AS user__home_state_latest
      , SUM(subq_32.buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_28.visits) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.ds__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_28.visit__referrer_id) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.ds__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(subq_28.user__home_state_latest) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.ds__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user__home_state_latest
        , FIRST_VALUE(subq_28.ds__day) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.ds__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__day
        , FIRST_VALUE(subq_28.metric_time__day) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.ds__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_28.user) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.ds__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_31.mf_internal_uuid AS mf_internal_uuid
        , subq_31.buys AS buys
      FROM (
        -- Join Standard Outputs
        -- Pass Only Elements: ['visits', 'visit__referrer_id', 'user__home_state_latest', 'ds__day', 'metric_time__day', 'user']
        SELECT
          subq_26.ds__day AS ds__day
          , subq_26.metric_time__day AS metric_time__day
          , subq_26.user AS user
          , subq_26.visit__referrer_id AS visit__referrer_id
          , subq_26.visits AS visits
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
        ) subq_26
        LEFT OUTER JOIN
          ***************************.dim_users_latest users_latest_src_28000
        ON
          subq_26.user = users_latest_src_28000.user_id
      ) subq_28
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS ds__day
          , user_id AS user
          , 1 AS buys
          , GEN_RANDOM_UUID() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_31
      ON
        (
          subq_28.user = subq_31.user
        ) AND (
          (
            subq_28.ds__day <= subq_31.ds__day
          ) AND (
            subq_28.ds__day > subq_31.ds__day - INTERVAL 7 day
          )
        )
    ) subq_32
    LEFT OUTER JOIN
      ***************************.dim_users_latest users_latest_src_28000
    ON
      subq_32.user = users_latest_src_28000.user_id
    GROUP BY
      subq_32.metric_time__day
      , subq_35.user__home_state_latest
  ) subq_36
  ON
    (
      subq_24.user__home_state_latest = subq_36.user__home_state_latest
    ) AND (
      subq_24.metric_time__day = subq_36.metric_time__day
    )
  GROUP BY
    COALESCE(subq_24.metric_time__day, subq_36.metric_time__day)
    , COALESCE(subq_24.user__home_state_latest, subq_36.user__home_state_latest)
) subq_37
