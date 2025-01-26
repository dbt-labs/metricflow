test_name: test_filter_with_conversion_metric
test_filename: test_metric_filter_rendering.py
sql_engine: BigQuery
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , user_id AS user
    , 1 AS visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  SELECT
    CAST(subq_49.buys AS FLOAT64) / CAST(NULLIF(subq_49.visits, 0) AS FLOAT64) AS user__visit_buy_conversion_rate
    , subq_35.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      user_id AS user
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_35
  LEFT OUTER JOIN (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_39.user, subq_48.user) AS user
      , MAX(subq_39.visits) AS visits
      , MAX(subq_48.buys) AS buys
    FROM (
      -- Read From CTE For node_id=sma_28019
      -- Pass Only Elements: ['visits', 'user']
      -- Aggregate Measures
      SELECT
        sma_28019_cte.user
        , SUM(visits) AS visits
      FROM sma_28019_cte sma_28019_cte
      GROUP BY
        user
    ) subq_39
    FULL OUTER JOIN (
      -- Find conversions for user within the range of INF
      -- Pass Only Elements: ['buys', 'user']
      -- Aggregate Measures
      SELECT
        subq_45.user
        , SUM(buys) AS buys
      FROM (
        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
        SELECT DISTINCT
          FIRST_VALUE(sma_28019_cte.visits) OVER (
            PARTITION BY
              subq_44.user
              , subq_44.metric_time__day
              , subq_44.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS visits
          , FIRST_VALUE(sma_28019_cte.metric_time__day) OVER (
            PARTITION BY
              subq_44.user
              , subq_44.metric_time__day
              , subq_44.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS metric_time__day
          , FIRST_VALUE(sma_28019_cte.user) OVER (
            PARTITION BY
              subq_44.user
              , subq_44.metric_time__day
              , subq_44.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , subq_44.mf_internal_uuid AS mf_internal_uuid
          , subq_44.buys AS buys
        FROM sma_28019_cte sma_28019_cte
        INNER JOIN (
          -- Read Elements From Semantic Model 'buys_source'
          -- Metric Time Dimension 'ds'
          -- Add column with generated UUID
          SELECT
            DATETIME_TRUNC(ds, day) AS metric_time__day
            , user_id AS user
            , 1 AS buys
            , GENERATE_UUID() AS mf_internal_uuid
          FROM ***************************.fct_buys buys_source_src_28000
        ) subq_44
        ON
          (
            sma_28019_cte.user = subq_44.user
          ) AND (
            (sma_28019_cte.metric_time__day <= subq_44.metric_time__day)
          )
      ) subq_45
      GROUP BY
        user
    ) subq_48
    ON
      subq_39.user = subq_48.user
    GROUP BY
      user
  ) subq_49
  ON
    subq_35.user = subq_49.user
) subq_52
WHERE user__visit_buy_conversion_rate > 2
