test_name: test_filter_with_conversion_metric
test_filename: test_metric_filter_rendering.py
sql_engine: Databricks
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listings']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
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
    CAST(subq_50.buys AS DOUBLE) / CAST(NULLIF(subq_50.visits, 0) AS DOUBLE) AS user__visit_buy_conversion_rate
    , subq_36.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      user_id AS user
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_36
  LEFT OUTER JOIN (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_40.user, subq_49.user) AS user
      , MAX(subq_40.visits) AS visits
      , MAX(subq_49.buys) AS buys
    FROM (
      -- Read From CTE For node_id=sma_28019
      -- Pass Only Elements: ['visits', 'user']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        sma_28019_cte.user
        , SUM(visits) AS visits
      FROM sma_28019_cte
      GROUP BY
        sma_28019_cte.user
    ) subq_40
    FULL OUTER JOIN (
      -- Find conversions for user within the range of INF
      -- Pass Only Elements: ['buys', 'user']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_46.user
        , SUM(buys) AS buys
      FROM (
        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
        SELECT DISTINCT
          FIRST_VALUE(sma_28019_cte.visits) OVER (
            PARTITION BY
              subq_45.user
              , subq_45.metric_time__day
              , subq_45.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS visits
          , FIRST_VALUE(sma_28019_cte.metric_time__day) OVER (
            PARTITION BY
              subq_45.user
              , subq_45.metric_time__day
              , subq_45.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS metric_time__day
          , FIRST_VALUE(sma_28019_cte.user) OVER (
            PARTITION BY
              subq_45.user
              , subq_45.metric_time__day
              , subq_45.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , subq_45.mf_internal_uuid AS mf_internal_uuid
          , subq_45.buys AS buys
        FROM sma_28019_cte
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
        ) subq_45
        ON
          (
            sma_28019_cte.user = subq_45.user
          ) AND (
            (sma_28019_cte.metric_time__day <= subq_45.metric_time__day)
          )
      ) subq_46
      GROUP BY
        subq_46.user
    ) subq_49
    ON
      subq_40.user = subq_49.user
    GROUP BY
      COALESCE(subq_40.user, subq_49.user)
  ) subq_50
  ON
    subq_36.user = subq_50.user
) subq_53
WHERE user__visit_buy_conversion_rate > 2
