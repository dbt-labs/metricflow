test_name: test_filter_with_conversion_metric
test_filename: test_metric_filter_rendering.py
sql_engine: BigQuery
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['__listings']
-- Aggregate Inputs for Simple Metrics
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , user_id AS user
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['__listings', 'user__visit_buy_conversion_rate']
  SELECT
    CAST(subq_60.__buys AS FLOAT64) / CAST(NULLIF(subq_60.__visits, 0) AS FLOAT64) AS user__visit_buy_conversion_rate
    , subq_43.__listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      user_id AS user
      , 1 AS __listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_43
  LEFT OUTER JOIN (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_48.user, subq_59.user) AS user
      , MAX(subq_48.__visits) AS __visits
      , MAX(subq_59.__buys) AS __buys
    FROM (
      -- Read From CTE For node_id=sma_28019
      -- Pass Only Elements: ['__visits', 'user']
      -- Pass Only Elements: ['__visits', 'user']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        sma_28019_cte.user
        , SUM(__visits) AS __visits
      FROM sma_28019_cte
      GROUP BY
        user
    ) subq_48
    FULL OUTER JOIN (
      -- Find conversions for user within the range of INF
      -- Pass Only Elements: ['__buys', 'user']
      -- Pass Only Elements: ['__buys', 'user']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        subq_55.user
        , SUM(__buys) AS __buys
      FROM (
        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
        SELECT DISTINCT
          FIRST_VALUE(sma_28019_cte.__visits) OVER (
            PARTITION BY
              subq_54.user
              , subq_54.metric_time__day
              , subq_54.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS __visits
          , FIRST_VALUE(sma_28019_cte.metric_time__day) OVER (
            PARTITION BY
              subq_54.user
              , subq_54.metric_time__day
              , subq_54.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS metric_time__day
          , FIRST_VALUE(sma_28019_cte.user) OVER (
            PARTITION BY
              subq_54.user
              , subq_54.metric_time__day
              , subq_54.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , subq_54.mf_internal_uuid AS mf_internal_uuid
          , subq_54.__buys AS __buys
        FROM sma_28019_cte
        INNER JOIN (
          -- Read Elements From Semantic Model 'buys_source'
          -- Metric Time Dimension 'ds'
          -- Add column with generated UUID
          SELECT
            DATETIME_TRUNC(ds, day) AS metric_time__day
            , user_id AS user
            , 1 AS __buys
            , GENERATE_UUID() AS mf_internal_uuid
          FROM ***************************.fct_buys buys_source_src_28000
        ) subq_54
        ON
          (
            sma_28019_cte.user = subq_54.user
          ) AND (
            (sma_28019_cte.metric_time__day <= subq_54.metric_time__day)
          )
      ) subq_55
      GROUP BY
        user
    ) subq_59
    ON
      subq_48.user = subq_59.user
    GROUP BY
      user
  ) subq_60
  ON
    subq_43.user = subq_60.user
) subq_64
WHERE user__visit_buy_conversion_rate > 2
