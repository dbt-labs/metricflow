test_name: test_conversion_metric_with_filter
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: DuckDB
---
-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  CAST(MAX(subq_32.buys) AS DOUBLE) / CAST(NULLIF(MAX(subq_21.visits), 0) AS DOUBLE) AS visit_buy_conversion_rate
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['visits',]
  -- Aggregate Measures
  SELECT
    SUM(visits) AS visits
  FROM (
    -- Read Elements From Semantic Model 'visits_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS visits
    FROM ***************************.fct_visits visits_source_src_28000
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
      FIRST_VALUE(subq_25.visits) OVER (
        PARTITION BY
          subq_28.user
          , subq_28.metric_time__day
          , subq_28.mf_internal_uuid
        ORDER BY subq_25.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(subq_25.metric_time__day) OVER (
        PARTITION BY
          subq_28.user
          , subq_28.metric_time__day
          , subq_28.mf_internal_uuid
        ORDER BY subq_25.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__day
      , FIRST_VALUE(subq_25.user) OVER (
        PARTITION BY
          subq_28.user
          , subq_28.metric_time__day
          , subq_28.mf_internal_uuid
        ORDER BY subq_25.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , subq_28.mf_internal_uuid AS mf_internal_uuid
      , subq_28.buys AS buys
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['visits', 'metric_time__day', 'user']
      SELECT
        metric_time__day
        , subq_23.user
        , visits
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) subq_23
      WHERE metric_time__day = '2020-01-01'
    ) subq_25
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
    ) subq_28
    ON
      (
        subq_25.user = subq_28.user
      ) AND (
        (subq_25.metric_time__day <= subq_28.metric_time__day)
      )
  ) subq_29
) subq_32