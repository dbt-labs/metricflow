-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , user__home_state_latest
  , CAST(buys AS DOUBLE PRECISION) / CAST(NULLIF(visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_39.metric_time__day, subq_58.metric_time__day) AS metric_time__day
    , COALESCE(subq_39.user__home_state_latest, subq_58.user__home_state_latest) AS user__home_state_latest
    , MAX(subq_39.visits) AS visits
    , MAX(subq_58.buys) AS buys
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
        subq_32.metric_time__day AS metric_time__day
        , subq_32.visit__referrer_id AS visit__referrer_id
        , users_latest_src_28000.home_state_latest AS user__home_state_latest
        , subq_32.visits AS visits
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
      ) subq_32
      LEFT OUTER JOIN
        ***************************.dim_users_latest users_latest_src_28000
      ON
        subq_32.user = users_latest_src_28000.user_id
    ) subq_36
    WHERE visit__referrer_id = '123456'
    GROUP BY
      metric_time__day
      , user__home_state_latest
  ) subq_39
  FULL OUTER JOIN (
    -- Join Standard Outputs
    -- Pass Only Elements: ['buys', 'user__home_state_latest', 'metric_time__day']
    -- Aggregate Measures
    -- Pass Only Elements: ['buys', 'user__home_state_latest', 'metric_time__day']
    SELECT
      subq_50.metric_time__day AS metric_time__day
      , users_latest_src_28000.home_state_latest AS user__home_state_latest
      , SUM(subq_50.buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_46.visits) OVER (
          PARTITION BY
            subq_49.user
            , subq_49.ds__day
            , subq_49.mf_internal_uuid
          ORDER BY subq_46.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_46.visit__referrer_id) OVER (
          PARTITION BY
            subq_49.user
            , subq_49.ds__day
            , subq_49.mf_internal_uuid
          ORDER BY subq_46.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(subq_46.user__home_state_latest) OVER (
          PARTITION BY
            subq_49.user
            , subq_49.ds__day
            , subq_49.mf_internal_uuid
          ORDER BY subq_46.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user__home_state_latest
        , FIRST_VALUE(subq_46.ds__day) OVER (
          PARTITION BY
            subq_49.user
            , subq_49.ds__day
            , subq_49.mf_internal_uuid
          ORDER BY subq_46.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__day
        , FIRST_VALUE(subq_46.metric_time__day) OVER (
          PARTITION BY
            subq_49.user
            , subq_49.ds__day
            , subq_49.mf_internal_uuid
          ORDER BY subq_46.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_46.user) OVER (
          PARTITION BY
            subq_49.user
            , subq_49.ds__day
            , subq_49.mf_internal_uuid
          ORDER BY subq_46.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_49.mf_internal_uuid AS mf_internal_uuid
        , subq_49.buys AS buys
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['visits', 'visit__referrer_id', 'user__home_state_latest', 'ds__day', 'metric_time__day', 'user']
        SELECT
          ds__day
          , metric_time__day
          , subq_44.user
          , visit__referrer_id
          , user__home_state_latest
          , visits
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_41.ds__day AS ds__day
            , subq_41.metric_time__day AS metric_time__day
            , subq_41.user AS user
            , subq_41.visit__referrer_id AS visit__referrer_id
            , users_latest_src_28000.home_state_latest AS user__home_state_latest
            , subq_41.visits AS visits
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
          ) subq_41
          LEFT OUTER JOIN
            ***************************.dim_users_latest users_latest_src_28000
          ON
            subq_41.user = users_latest_src_28000.user_id
        ) subq_44
        WHERE visit__referrer_id = '123456'
      ) subq_46
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
      ) subq_49
      ON
        (
          subq_46.user = subq_49.user
        ) AND (
          (
            subq_46.ds__day <= subq_49.ds__day
          ) AND (
            subq_46.ds__day > subq_49.ds__day - MAKE_INTERVAL(days => 7)
          )
        )
    ) subq_50
    LEFT OUTER JOIN
      ***************************.dim_users_latest users_latest_src_28000
    ON
      subq_50.user = users_latest_src_28000.user_id
    GROUP BY
      subq_50.metric_time__day
      , users_latest_src_28000.home_state_latest
  ) subq_58
  ON
    (
      subq_39.user__home_state_latest = subq_58.user__home_state_latest
    ) AND (
      subq_39.metric_time__day = subq_58.metric_time__day
    )
  GROUP BY
    COALESCE(subq_39.metric_time__day, subq_58.metric_time__day)
    , COALESCE(subq_39.user__home_state_latest, subq_58.user__home_state_latest)
) subq_59
