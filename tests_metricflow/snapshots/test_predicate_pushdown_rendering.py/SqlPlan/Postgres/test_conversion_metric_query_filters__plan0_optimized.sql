test_name: test_conversion_metric_query_filters
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a simple predicate on a conversion metric.
sql_engine: Postgres
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , user_id AS user
    , referrer_id AS visit__referrer_id
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
)

, rss_28028_cte AS (
  -- Read Elements From Semantic Model 'users_latest'
  SELECT
    home_state_latest
    , user_id AS user
  FROM ***************************.dim_users_latest users_latest_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , user__home_state_latest AS user__home_state_latest
  , CAST(__buys AS DOUBLE PRECISION) / CAST(NULLIF(__visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_34.metric_time__day, subq_49.metric_time__day) AS metric_time__day
    , COALESCE(subq_34.user__home_state_latest, subq_49.user__home_state_latest) AS user__home_state_latest
    , MAX(subq_34.__visits) AS __visits
    , MAX(subq_49.__buys) AS __buys
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['__visits', 'user__home_state_latest', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , user__home_state_latest
      , SUM(visits) AS __visits
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['__visits', 'user__home_state_latest', 'visit__referrer_id', 'metric_time__day']
      SELECT
        sma_28019_cte.metric_time__day AS metric_time__day
        , sma_28019_cte.visit__referrer_id AS visit__referrer_id
        , rss_28028_cte.home_state_latest AS user__home_state_latest
        , sma_28019_cte.__visits AS visits
      FROM sma_28019_cte
      LEFT OUTER JOIN
        rss_28028_cte
      ON
        sma_28019_cte.user = rss_28028_cte.user
    ) subq_31
    WHERE visit__referrer_id = '123456'
    GROUP BY
      metric_time__day
      , user__home_state_latest
  ) subq_34
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['__buys', 'user__home_state_latest', 'metric_time__day']
    -- Pass Only Elements: ['__buys', 'user__home_state_latest', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , user__home_state_latest
      , SUM(__buys) AS __buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_41.__visits) OVER (
          PARTITION BY
            subq_44.user
            , subq_44.metric_time__day
            , subq_44.mf_internal_uuid
          ORDER BY subq_41.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS __visits
        , FIRST_VALUE(subq_41.visit__referrer_id) OVER (
          PARTITION BY
            subq_44.user
            , subq_44.metric_time__day
            , subq_44.mf_internal_uuid
          ORDER BY subq_41.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(subq_41.user__home_state_latest) OVER (
          PARTITION BY
            subq_44.user
            , subq_44.metric_time__day
            , subq_44.mf_internal_uuid
          ORDER BY subq_41.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user__home_state_latest
        , FIRST_VALUE(subq_41.metric_time__day) OVER (
          PARTITION BY
            subq_44.user
            , subq_44.metric_time__day
            , subq_44.mf_internal_uuid
          ORDER BY subq_41.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_41.user) OVER (
          PARTITION BY
            subq_44.user
            , subq_44.metric_time__day
            , subq_44.mf_internal_uuid
          ORDER BY subq_41.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_44.mf_internal_uuid AS mf_internal_uuid
        , subq_44.__buys AS __buys
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'user__home_state_latest', 'metric_time__day', 'user']
        SELECT
          metric_time__day
          , subq_39.user
          , visit__referrer_id
          , user__home_state_latest
          , visits AS __visits
        FROM (
          -- Join Standard Outputs
          -- Pass Only Elements: ['__visits', 'visit__referrer_id', 'user__home_state_latest', 'metric_time__day', 'user']
          SELECT
            sma_28019_cte.metric_time__day AS metric_time__day
            , sma_28019_cte.user AS user
            , sma_28019_cte.visit__referrer_id AS visit__referrer_id
            , rss_28028_cte.home_state_latest AS user__home_state_latest
            , sma_28019_cte.__visits AS visits
          FROM sma_28019_cte
          LEFT OUTER JOIN
            rss_28028_cte
          ON
            sma_28019_cte.user = rss_28028_cte.user
        ) subq_39
        WHERE visit__referrer_id = '123456'
      ) subq_41
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS __buys
          , GEN_RANDOM_UUID() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_44
      ON
        (
          subq_41.user = subq_44.user
        ) AND (
          (
            subq_41.metric_time__day <= subq_44.metric_time__day
          ) AND (
            subq_41.metric_time__day > subq_44.metric_time__day - MAKE_INTERVAL(days => 7)
          )
        )
    ) subq_45
    GROUP BY
      metric_time__day
      , user__home_state_latest
  ) subq_49
  ON
    (
      subq_34.user__home_state_latest = subq_49.user__home_state_latest
    ) AND (
      subq_34.metric_time__day = subq_49.metric_time__day
    )
  GROUP BY
    COALESCE(subq_34.metric_time__day, subq_49.metric_time__day)
    , COALESCE(subq_34.user__home_state_latest, subq_49.user__home_state_latest)
) subq_50
