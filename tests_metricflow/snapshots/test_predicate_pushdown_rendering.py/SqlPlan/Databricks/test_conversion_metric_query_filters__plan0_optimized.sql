test_name: test_conversion_metric_query_filters
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a simple predicate on a conversion metric.
sql_engine: Databricks
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , user__home_state_latest
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_26.metric_time__day, nr_subq_38.metric_time__day) AS metric_time__day
    , COALESCE(nr_subq_26.user__home_state_latest, nr_subq_38.user__home_state_latest) AS user__home_state_latest
    , MAX(nr_subq_26.visits) AS visits
    , MAX(nr_subq_38.buys) AS buys
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
        , nr_subq_20.metric_time__day AS metric_time__day
        , nr_subq_20.visit__referrer_id AS visit__referrer_id
        , nr_subq_20.visits AS visits
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , referrer_id AS visit__referrer_id
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) nr_subq_20
      LEFT OUTER JOIN
        ***************************.dim_users_latest users_latest_src_28000
      ON
        nr_subq_20.user = users_latest_src_28000.user_id
    ) nr_subq_23
    WHERE visit__referrer_id = '123456'
    GROUP BY
      metric_time__day
      , user__home_state_latest
  ) nr_subq_26
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
        FIRST_VALUE(nr_subq_32.visits) OVER (
          PARTITION BY
            nr_subq_34.user
            , nr_subq_34.metric_time__day
            , nr_subq_34.mf_internal_uuid
          ORDER BY nr_subq_32.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(nr_subq_32.visit__referrer_id) OVER (
          PARTITION BY
            nr_subq_34.user
            , nr_subq_34.metric_time__day
            , nr_subq_34.mf_internal_uuid
          ORDER BY nr_subq_32.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(nr_subq_32.user__home_state_latest) OVER (
          PARTITION BY
            nr_subq_34.user
            , nr_subq_34.metric_time__day
            , nr_subq_34.mf_internal_uuid
          ORDER BY nr_subq_32.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user__home_state_latest
        , FIRST_VALUE(nr_subq_32.metric_time__day) OVER (
          PARTITION BY
            nr_subq_34.user
            , nr_subq_34.metric_time__day
            , nr_subq_34.mf_internal_uuid
          ORDER BY nr_subq_32.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(nr_subq_32.user) OVER (
          PARTITION BY
            nr_subq_34.user
            , nr_subq_34.metric_time__day
            , nr_subq_34.mf_internal_uuid
          ORDER BY nr_subq_32.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , nr_subq_34.mf_internal_uuid AS mf_internal_uuid
        , nr_subq_34.buys AS buys
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['visits', 'visit__referrer_id', 'user__home_state_latest', 'metric_time__day', 'user']
        SELECT
          metric_time__day
          , nr_subq_30.user
          , visit__referrer_id
          , user__home_state_latest
          , visits
        FROM (
          -- Join Standard Outputs
          SELECT
            users_latest_src_28000.home_state_latest AS user__home_state_latest
            , nr_subq_27.metric_time__day AS metric_time__day
            , nr_subq_27.user AS user
            , nr_subq_27.visit__referrer_id AS visit__referrer_id
            , nr_subq_27.visits AS visits
          FROM (
            -- Read Elements From Semantic Model 'visits_source'
            -- Metric Time Dimension 'ds'
            SELECT
              DATE_TRUNC('day', ds) AS metric_time__day
              , user_id AS user
              , referrer_id AS visit__referrer_id
              , 1 AS visits
            FROM ***************************.fct_visits visits_source_src_28000
          ) nr_subq_27
          LEFT OUTER JOIN
            ***************************.dim_users_latest users_latest_src_28000
          ON
            nr_subq_27.user = users_latest_src_28000.user_id
        ) nr_subq_30
        WHERE visit__referrer_id = '123456'
      ) nr_subq_32
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS buys
          , UUID() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) nr_subq_34
      ON
        (
          nr_subq_32.user = nr_subq_34.user
        ) AND (
          (
            nr_subq_32.metric_time__day <= nr_subq_34.metric_time__day
          ) AND (
            nr_subq_32.metric_time__day > DATEADD(day, -7, nr_subq_34.metric_time__day)
          )
        )
    ) nr_subq_35
    GROUP BY
      metric_time__day
      , user__home_state_latest
  ) nr_subq_38
  ON
    (
      nr_subq_26.user__home_state_latest = nr_subq_38.user__home_state_latest
    ) AND (
      nr_subq_26.metric_time__day = nr_subq_38.metric_time__day
    )
  GROUP BY
    COALESCE(nr_subq_26.metric_time__day, nr_subq_38.metric_time__day)
    , COALESCE(nr_subq_26.user__home_state_latest, nr_subq_38.user__home_state_latest)
) nr_subq_39
