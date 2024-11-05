-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , user__home_state_latest
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_30.metric_time__day, subq_44.metric_time__day) AS metric_time__day
    , COALESCE(subq_30.user__home_state_latest, subq_44.user__home_state_latest) AS user__home_state_latest
    , MAX(subq_30.visits) AS visits
    , MAX(subq_44.buys) AS buys
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
      SELECT
        users_latest_src_28000.home_state_latest AS user__home_state_latest
        , subq_24.metric_time__day AS metric_time__day
        , subq_24.visit__referrer_id AS visit__referrer_id
        , subq_24.visits AS visits
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , referrer_id AS visit__referrer_id
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) subq_24
      LEFT OUTER JOIN
        ***************************.dim_users_latest users_latest_src_28000
      ON
        subq_24.user = users_latest_src_28000.user_id
    ) subq_27
    WHERE visit__referrer_id = '123456'
    GROUP BY
      metric_time__day
      , user__home_state_latest
  ) subq_30
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['buys', 'user__home_state_latest', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , user__home_state_latest
      , SUM(buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_37.visits) OVER (
          PARTITION BY
            subq_40.user
            , subq_40.metric_time__day
            , subq_40.mf_internal_uuid
          ORDER BY subq_37.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_37.visit__referrer_id) OVER (
          PARTITION BY
            subq_40.user
            , subq_40.metric_time__day
            , subq_40.mf_internal_uuid
          ORDER BY subq_37.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(subq_37.user__home_state_latest) OVER (
          PARTITION BY
            subq_40.user
            , subq_40.metric_time__day
            , subq_40.mf_internal_uuid
          ORDER BY subq_37.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user__home_state_latest
        , FIRST_VALUE(subq_37.metric_time__day) OVER (
          PARTITION BY
            subq_40.user
            , subq_40.metric_time__day
            , subq_40.mf_internal_uuid
          ORDER BY subq_37.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_37.user) OVER (
          PARTITION BY
            subq_40.user
            , subq_40.metric_time__day
            , subq_40.mf_internal_uuid
          ORDER BY subq_37.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_40.mf_internal_uuid AS mf_internal_uuid
        , subq_40.buys AS buys
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['visits', 'visit__referrer_id', 'user__home_state_latest', 'metric_time__day', 'user']
        SELECT
          metric_time__day
          , subq_35.user
          , visit__referrer_id
          , user__home_state_latest
          , visits
        FROM (
          -- Join Standard Outputs
          SELECT
            users_latest_src_28000.home_state_latest AS user__home_state_latest
            , subq_32.metric_time__day AS metric_time__day
            , subq_32.user AS user
            , subq_32.visit__referrer_id AS visit__referrer_id
            , subq_32.visits AS visits
          FROM (
            -- Read Elements From Semantic Model 'visits_source'
            -- Metric Time Dimension 'ds'
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
        ) subq_35
        WHERE visit__referrer_id = '123456'
      ) subq_37
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS buys
          , GEN_RANDOM_UUID() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_40
      ON
        (
          subq_37.user = subq_40.user
        ) AND (
          (
            subq_37.metric_time__day <= subq_40.metric_time__day
          ) AND (
            subq_37.metric_time__day > subq_40.metric_time__day - INTERVAL 7 day
          )
        )
    ) subq_41
    GROUP BY
      metric_time__day
      , user__home_state_latest
  ) subq_44
  ON
    (
      subq_30.user__home_state_latest = subq_44.user__home_state_latest
    ) AND (
      subq_30.metric_time__day = subq_44.metric_time__day
    )
  GROUP BY
    COALESCE(subq_30.metric_time__day, subq_44.metric_time__day)
    , COALESCE(subq_30.user__home_state_latest, subq_44.user__home_state_latest)
) subq_45
