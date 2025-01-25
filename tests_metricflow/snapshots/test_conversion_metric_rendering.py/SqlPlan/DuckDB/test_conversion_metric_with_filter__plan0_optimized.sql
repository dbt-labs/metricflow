test_name: test_conversion_metric_with_filter
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: DuckDB
---
-- Combine Aggregated Outputs
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
  CAST(MAX(subq_30.buys) AS DOUBLE) / CAST(NULLIF(MAX(subq_20.visits), 0) AS DOUBLE) AS visit_buy_conversion_rate
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['visits',]
  -- Aggregate Measures
  SELECT
    SUM(visits) AS visits
  FROM (
    -- Read From CTE For node_id=sma_28019
    SELECT
      metric_time__day
      , sma_28019_cte.user
      , visits
    FROM sma_28019_cte sma_28019_cte
  ) subq_17
  WHERE metric_time__day = '2020-01-01'
) subq_20
CROSS JOIN (
  -- Find conversions for user within the range of INF
  -- Pass Only Elements: ['buys',]
  -- Aggregate Measures
  SELECT
    SUM(buys) AS buys
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      FIRST_VALUE(subq_23.visits) OVER (
        PARTITION BY
          subq_26.user
          , subq_26.metric_time__day
          , subq_26.mf_internal_uuid
        ORDER BY subq_23.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(subq_23.metric_time__day) OVER (
        PARTITION BY
          subq_26.user
          , subq_26.metric_time__day
          , subq_26.mf_internal_uuid
        ORDER BY subq_23.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__day
      , FIRST_VALUE(subq_23.user) OVER (
        PARTITION BY
          subq_26.user
          , subq_26.metric_time__day
          , subq_26.mf_internal_uuid
        ORDER BY subq_23.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , subq_26.mf_internal_uuid AS mf_internal_uuid
      , subq_26.buys AS buys
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['visits', 'metric_time__day', 'user']
      SELECT
        metric_time__day
        , subq_21.user
        , visits
      FROM (
        -- Read From CTE For node_id=sma_28019
        SELECT
          metric_time__day
          , sma_28019_cte.user
          , visits
        FROM sma_28019_cte sma_28019_cte
      ) subq_21
      WHERE metric_time__day = '2020-01-01'
    ) subq_23
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
    ) subq_26
    ON
      (
        subq_23.user = subq_26.user
      ) AND (
        (subq_23.metric_time__day <= subq_26.metric_time__day)
      )
  ) subq_27
) subq_30
