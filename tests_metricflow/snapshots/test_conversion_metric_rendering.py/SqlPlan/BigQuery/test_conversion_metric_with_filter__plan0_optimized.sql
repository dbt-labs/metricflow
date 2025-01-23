test_name: test_conversion_metric_with_filter
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: BigQuery
---
-- Combine Aggregated Outputs
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
  CAST(MAX(subq_31.buys) AS FLOAT64) / CAST(NULLIF(MAX(subq_21.visits), 0) AS FLOAT64) AS visit_buy_conversion_rate
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
      , visits
    FROM sma_28019_cte sma_28019_cte
  ) subq_18
  WHERE metric_time__day = '2020-01-01'
) subq_21
CROSS JOIN (
  -- Find conversions for user within the range of INF
  -- Pass Only Elements: ['buys',]
  -- Aggregate Measures
  SELECT
    SUM(buys) AS buys
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      FIRST_VALUE(subq_24.visits) OVER (
        PARTITION BY
          subq_27.user
          , subq_27.metric_time__day
          , subq_27.mf_internal_uuid
        ORDER BY subq_24.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(subq_24.metric_time__day) OVER (
        PARTITION BY
          subq_27.user
          , subq_27.metric_time__day
          , subq_27.mf_internal_uuid
        ORDER BY subq_24.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__day
      , FIRST_VALUE(subq_24.user) OVER (
        PARTITION BY
          subq_27.user
          , subq_27.metric_time__day
          , subq_27.mf_internal_uuid
        ORDER BY subq_24.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , subq_27.mf_internal_uuid AS mf_internal_uuid
      , subq_27.buys AS buys
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['visits', 'metric_time__day', 'user']
      SELECT
        metric_time__day
        , subq_22.user
        , visits
      FROM (
        -- Read From CTE For node_id=sma_28019
        SELECT
          metric_time__day
          , sma_28019_cte.user
          , visits
        FROM sma_28019_cte sma_28019_cte
      ) subq_22
      WHERE metric_time__day = '2020-01-01'
    ) subq_24
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
    ) subq_27
    ON
      (
        subq_24.user = subq_27.user
      ) AND (
        (subq_24.metric_time__day <= subq_27.metric_time__day)
      )
  ) subq_28
) subq_31
