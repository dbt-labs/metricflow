test_name: test_conversion_metric_query_filters
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests pushdown optimizer behavior for a simple predicate on a conversion metric.
sql_engine: ClickHouse
---
WITH sma_28019_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , user_id AS user
    , referrer_id AS visit__referrer_id
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
)

, rss_28028_cte AS (
  SELECT
    home_state_latest
    , user_id AS user
  FROM ***************************.dim_users_latest users_latest_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , user__home_state_latest AS user__home_state_latest
  , CAST(__buys AS Nullable(Float64)) / CAST(NULLIF(__visits, 0) AS Nullable(Float64)) AS visit_buy_conversion_rate_7days
FROM (
  SELECT
    COALESCE(subq_34.metric_time__day, subq_49.metric_time__day) AS metric_time__day
    , COALESCE(subq_34.user__home_state_latest, subq_49.user__home_state_latest) AS user__home_state_latest
    , MAX(subq_34.__visits) AS __visits
    , MAX(subq_49.__buys) AS __buys
  FROM (
    SELECT
      metric_time__day
      , user__home_state_latest
      , SUM(__visits) AS __visits
    FROM (
      SELECT
        metric_time__day
        , user__home_state_latest
        , visits AS __visits
      FROM (
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
    ) subq_33
    GROUP BY
      metric_time__day
      , user__home_state_latest
  ) subq_34
  FULL OUTER JOIN (
    SELECT
      metric_time__day
      , user__home_state_latest
      , SUM(__buys) AS __buys
    FROM (
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
        SELECT
          metric_time__day
          , subq_39.user
          , visit__referrer_id
          , user__home_state_latest
          , visits AS __visits
        FROM (
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
        SELECT
          toStartOfDay(ds) AS metric_time__day
          , user_id AS user
          , 1 AS __buys
          , generateUUIDv4() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_44
      ON
        (
          subq_41.user = subq_44.user
        ) AND (
          (
            subq_41.metric_time__day <= subq_44.metric_time__day
          ) AND (
            subq_41.metric_time__day > addDays(subq_44.metric_time__day, -7)
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
