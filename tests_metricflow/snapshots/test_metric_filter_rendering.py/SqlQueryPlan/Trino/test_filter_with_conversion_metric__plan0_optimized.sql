test_name: test_filter_with_conversion_metric
test_filename: test_metric_filter_rendering.py
sql_engine: Trino
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , user_id AS user
    , 1 AS visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  SELECT
    CAST(subq_38.buys AS DOUBLE) / CAST(NULLIF(subq_38.visits, 0) AS DOUBLE) AS user__visit_buy_conversion_rate
    , subq_24.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      user_id AS user
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_24
  LEFT OUTER JOIN (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_28.user, subq_37.user) AS user
      , MAX(subq_28.visits) AS visits
      , MAX(subq_37.buys) AS buys
    FROM (
      -- Read From CTE For node_id=sma_28019
      -- Pass Only Elements: ['visits', 'user']
      -- Aggregate Measures
      SELECT
        sma_28019_cte.user
        , SUM(visits) AS visits
      FROM sma_28019_cte sma_28019_cte
      GROUP BY
        sma_28019_cte.user
    ) subq_28
    FULL OUTER JOIN (
      -- Find conversions for user within the range of INF
      -- Pass Only Elements: ['buys', 'user']
      -- Aggregate Measures
      SELECT
        subq_34.user
        , SUM(buys) AS buys
      FROM (
        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
        SELECT DISTINCT
          FIRST_VALUE(sma_28019_cte.visits) OVER (
            PARTITION BY
              subq_33.user
              , subq_33.metric_time__day
              , subq_33.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS visits
          , FIRST_VALUE(sma_28019_cte.metric_time__day) OVER (
            PARTITION BY
              subq_33.user
              , subq_33.metric_time__day
              , subq_33.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS metric_time__day
          , FIRST_VALUE(sma_28019_cte.user) OVER (
            PARTITION BY
              subq_33.user
              , subq_33.metric_time__day
              , subq_33.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , subq_33.mf_internal_uuid AS mf_internal_uuid
          , subq_33.buys AS buys
        FROM sma_28019_cte sma_28019_cte
        INNER JOIN (
          -- Read Elements From Semantic Model 'buys_source'
          -- Metric Time Dimension 'ds'
          -- Add column with generated UUID
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , user_id AS user
            , 1 AS buys
            , uuid() AS mf_internal_uuid
          FROM ***************************.fct_buys buys_source_src_28000
        ) subq_33
        ON
          (
            sma_28019_cte.user = subq_33.user
          ) AND (
            (sma_28019_cte.metric_time__day <= subq_33.metric_time__day)
          )
      ) subq_34
      GROUP BY
        subq_34.user
    ) subq_37
    ON
      subq_28.user = subq_37.user
    GROUP BY
      COALESCE(subq_28.user, subq_37.user)
  ) subq_38
  ON
    subq_24.user = subq_38.user
) subq_41
WHERE user__visit_buy_conversion_rate > 2
