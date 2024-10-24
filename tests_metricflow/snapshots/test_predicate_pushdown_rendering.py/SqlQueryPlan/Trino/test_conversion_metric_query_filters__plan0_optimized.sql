-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , user__home_state_latest
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_34.metric_time__day, subq_48.metric_time__day) AS metric_time__day
    , COALESCE(subq_34.user__home_state_latest, subq_48.user__home_state_latest) AS user__home_state_latest
    , MAX(subq_34.visits) AS visits
    , MAX(subq_48.buys) AS buys
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
        subq_27.metric_time__day AS metric_time__day
        , subq_27.visit__referrer_id AS visit__referrer_id
        , users_latest_src_28000.home_state_latest AS user__home_state_latest
        , subq_27.visits AS visits
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
      ) subq_27
      LEFT OUTER JOIN
        ***************************.dim_users_latest users_latest_src_28000
      ON
        subq_27.user = users_latest_src_28000.user_id
    ) subq_31
    WHERE visit__referrer_id = '123456'
    GROUP BY
      metric_time__day
      , user__home_state_latest
  ) subq_34
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
        FIRST_VALUE(subq_41.visits) OVER (
          PARTITION BY
            subq_44.user
            , subq_44.ds__day
            , subq_44.mf_internal_uuid
          ORDER BY subq_41.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_41.visit__referrer_id) OVER (
          PARTITION BY
            subq_44.user
            , subq_44.ds__day
            , subq_44.mf_internal_uuid
          ORDER BY subq_41.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(subq_41.user__home_state_latest) OVER (
          PARTITION BY
            subq_44.user
            , subq_44.ds__day
            , subq_44.mf_internal_uuid
          ORDER BY subq_41.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user__home_state_latest
        , FIRST_VALUE(subq_41.ds__day) OVER (
          PARTITION BY
            subq_44.user
            , subq_44.ds__day
            , subq_44.mf_internal_uuid
          ORDER BY subq_41.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__day
        , FIRST_VALUE(subq_41.metric_time__day) OVER (
          PARTITION BY
            subq_44.user
            , subq_44.ds__day
            , subq_44.mf_internal_uuid
          ORDER BY subq_41.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_41.user) OVER (
          PARTITION BY
            subq_44.user
            , subq_44.ds__day
            , subq_44.mf_internal_uuid
          ORDER BY subq_41.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_44.mf_internal_uuid AS mf_internal_uuid
        , subq_44.buys AS buys
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['visits', 'visit__referrer_id', 'user__home_state_latest', 'ds__day', 'metric_time__day', 'user']
        SELECT
          ds__day
          , metric_time__day
          , subq_39.user
          , visit__referrer_id
          , user__home_state_latest
          , visits
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_36.ds__day AS ds__day
            , subq_36.metric_time__day AS metric_time__day
            , subq_36.user AS user
            , subq_36.visit__referrer_id AS visit__referrer_id
            , users_latest_src_28000.home_state_latest AS user__home_state_latest
            , subq_36.visits AS visits
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
          ) subq_36
          LEFT OUTER JOIN
            ***************************.dim_users_latest users_latest_src_28000
          ON
            subq_36.user = users_latest_src_28000.user_id
        ) subq_39
        WHERE visit__referrer_id = '123456'
      ) subq_41
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS ds__day
          , user_id AS user
          , 1 AS buys
          , uuid() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_44
      ON
        (
          subq_41.user = subq_44.user
        ) AND (
          (
            subq_41.ds__day <= subq_44.ds__day
          ) AND (
            subq_41.ds__day > DATE_ADD('day', -7, subq_44.ds__day)
          )
        )
    ) subq_45
    GROUP BY
      metric_time__day
      , user__home_state_latest
  ) subq_48
  ON
    (
      subq_34.user__home_state_latest = subq_48.user__home_state_latest
    ) AND (
      subq_34.metric_time__day = subq_48.metric_time__day
    )
  GROUP BY
    COALESCE(subq_34.metric_time__day, subq_48.metric_time__day)
    , COALESCE(subq_34.user__home_state_latest, subq_48.user__home_state_latest)
) subq_49
