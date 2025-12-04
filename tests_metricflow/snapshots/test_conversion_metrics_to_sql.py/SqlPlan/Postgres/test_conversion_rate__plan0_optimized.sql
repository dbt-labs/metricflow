test_name: test_conversion_rate
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric data flow plan rendering.
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

SELECT
  visit__referrer_id AS visit__referrer_id
  , CAST(__buys AS DOUBLE PRECISION) / CAST(NULLIF(__visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_22.visit__referrer_id, subq_33.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_22.__visits) AS __visits
    , MAX(subq_33.__buys) AS __buys
  FROM (
    -- Read From CTE For node_id=sma_28019
    -- Pass Only Elements: ['__visits', 'visit__referrer_id']
    -- Pass Only Elements: ['__visits', 'visit__referrer_id']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      visit__referrer_id
      , SUM(__visits) AS __visits
    FROM sma_28019_cte
    GROUP BY
      visit__referrer_id
  ) subq_22
  FULL OUTER JOIN (
    -- Find conversions for user within the range of INF
    -- Pass Only Elements: ['__buys', 'visit__referrer_id']
    -- Pass Only Elements: ['__buys', 'visit__referrer_id']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      visit__referrer_id
      , SUM(__buys) AS __buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(sma_28019_cte.__visits) OVER (
          PARTITION BY
            subq_28.user
            , subq_28.metric_time__day
            , subq_28.mf_internal_uuid
          ORDER BY sma_28019_cte.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS __visits
        , FIRST_VALUE(sma_28019_cte.visit__referrer_id) OVER (
          PARTITION BY
            subq_28.user
            , subq_28.metric_time__day
            , subq_28.mf_internal_uuid
          ORDER BY sma_28019_cte.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(sma_28019_cte.metric_time__day) OVER (
          PARTITION BY
            subq_28.user
            , subq_28.metric_time__day
            , subq_28.mf_internal_uuid
          ORDER BY sma_28019_cte.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(sma_28019_cte.user) OVER (
          PARTITION BY
            subq_28.user
            , subq_28.metric_time__day
            , subq_28.mf_internal_uuid
          ORDER BY sma_28019_cte.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_28.mf_internal_uuid AS mf_internal_uuid
        , subq_28.__buys AS __buys
      FROM sma_28019_cte
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
      ) subq_28
      ON
        (
          sma_28019_cte.user = subq_28.user
        ) AND (
          (sma_28019_cte.metric_time__day <= subq_28.metric_time__day)
        )
    ) subq_29
    GROUP BY
      visit__referrer_id
  ) subq_33
  ON
    subq_22.visit__referrer_id = subq_33.visit__referrer_id
  GROUP BY
    COALESCE(subq_22.visit__referrer_id, subq_33.visit__referrer_id)
) subq_34
