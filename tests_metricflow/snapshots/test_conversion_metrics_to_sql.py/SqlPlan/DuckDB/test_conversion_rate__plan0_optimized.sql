test_name: test_conversion_rate
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric data flow plan rendering.
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , user_id AS user
    , referrer_id AS visit__referrer_id
    , 1 AS visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  visit__referrer_id AS visit__referrer_id
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_17.visit__referrer_id, subq_26.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_17.visits) AS visits
    , MAX(subq_26.buys) AS buys
  FROM (
    -- Read From CTE For node_id=sma_28019
    -- Pass Only Elements: ['visits', 'visit__referrer_id']
    -- Aggregate Measures
    SELECT
      visit__referrer_id
      , SUM(visits) AS visits
    FROM sma_28019_cte sma_28019_cte
    GROUP BY
      visit__referrer_id
  ) subq_17
  FULL OUTER JOIN (
    -- Find conversions for user within the range of INF
    -- Pass Only Elements: ['buys', 'visit__referrer_id']
    -- Aggregate Measures
    SELECT
      visit__referrer_id
      , SUM(buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(sma_28019_cte.visits) OVER (
          PARTITION BY
            subq_22.user
            , subq_22.metric_time__day
            , subq_22.mf_internal_uuid
          ORDER BY sma_28019_cte.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(sma_28019_cte.visit__referrer_id) OVER (
          PARTITION BY
            subq_22.user
            , subq_22.metric_time__day
            , subq_22.mf_internal_uuid
          ORDER BY sma_28019_cte.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(sma_28019_cte.metric_time__day) OVER (
          PARTITION BY
            subq_22.user
            , subq_22.metric_time__day
            , subq_22.mf_internal_uuid
          ORDER BY sma_28019_cte.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(sma_28019_cte.user) OVER (
          PARTITION BY
            subq_22.user
            , subq_22.metric_time__day
            , subq_22.mf_internal_uuid
          ORDER BY sma_28019_cte.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_22.mf_internal_uuid AS mf_internal_uuid
        , subq_22.buys AS buys
      FROM sma_28019_cte sma_28019_cte
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
      ) subq_22
      ON
        (
          sma_28019_cte.user = subq_22.user
        ) AND (
          (sma_28019_cte.metric_time__day <= subq_22.metric_time__day)
        )
    ) subq_23
    GROUP BY
      visit__referrer_id
  ) subq_26
  ON
    subq_17.visit__referrer_id = subq_26.visit__referrer_id
  GROUP BY
    COALESCE(subq_17.visit__referrer_id, subq_26.visit__referrer_id)
) subq_27