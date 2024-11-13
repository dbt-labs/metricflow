test_name: test_filter_with_conversion_metric
test_filename: test_metric_filter_rendering.py
sql_engine: Postgres
---
-- Read From CTE For node_id=cm_4
WITH cm_3_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    subq_39.user
    , CAST(buys AS DOUBLE PRECISION) / CAST(NULLIF(visits, 0) AS DOUBLE PRECISION) AS user__visit_buy_conversion_rate
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_28.user, subq_38.user) AS user
      , MAX(subq_28.visits) AS visits
      , MAX(subq_38.buys) AS buys
    FROM (
      -- Aggregate Measures
      SELECT
        subq_27.user
        , SUM(visits) AS visits
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['visits', 'user']
        SELECT
          user_id AS user
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) subq_27
      GROUP BY
        subq_27.user
    ) subq_28
    FULL OUTER JOIN (
      -- Find conversions for user within the range of INF
      -- Pass Only Elements: ['buys', 'user']
      -- Aggregate Measures
      SELECT
        subq_35.user
        , SUM(buys) AS buys
      FROM (
        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
        SELECT DISTINCT
          FIRST_VALUE(subq_31.visits) OVER (
            PARTITION BY
              subq_34.user
              , subq_34.metric_time__day
              , subq_34.mf_internal_uuid
            ORDER BY subq_31.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS visits
          , FIRST_VALUE(subq_31.metric_time__day) OVER (
            PARTITION BY
              subq_34.user
              , subq_34.metric_time__day
              , subq_34.mf_internal_uuid
            ORDER BY subq_31.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS metric_time__day
          , FIRST_VALUE(subq_31.user) OVER (
            PARTITION BY
              subq_34.user
              , subq_34.metric_time__day
              , subq_34.mf_internal_uuid
            ORDER BY subq_31.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , subq_34.mf_internal_uuid AS mf_internal_uuid
          , subq_34.buys AS buys
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['visits', 'metric_time__day', 'user']
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , user_id AS user
            , 1 AS visits
          FROM ***************************.fct_visits visits_source_src_28000
        ) subq_31
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
        ) subq_34
        ON
          (
            subq_31.user = subq_34.user
          ) AND (
            (subq_31.metric_time__day <= subq_34.metric_time__day)
          )
      ) subq_35
      GROUP BY
        subq_35.user
    ) subq_38
    ON
      subq_28.user = subq_38.user
    GROUP BY
      COALESCE(subq_28.user, subq_38.user)
  ) subq_39
)

, cm_4_cte AS (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['listings',]
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(listings) AS listings
  FROM (
    -- Join Standard Outputs
    SELECT
      cm_3_cte.user__visit_buy_conversion_rate AS user__visit_buy_conversion_rate
      , subq_24.listings AS listings
    FROM (
      -- Read Elements From Semantic Model 'listings_latest'
      -- Metric Time Dimension 'ds'
      SELECT
        user_id AS user
        , 1 AS listings
      FROM ***************************.dim_listings_latest listings_latest_src_28000
    ) subq_24
    LEFT OUTER JOIN
      cm_3_cte cm_3_cte
    ON
      subq_24.user = cm_3_cte.user
  ) subq_42
  WHERE user__visit_buy_conversion_rate > 2
)

SELECT
  listings AS listings
FROM cm_4_cte cm_4_cte
