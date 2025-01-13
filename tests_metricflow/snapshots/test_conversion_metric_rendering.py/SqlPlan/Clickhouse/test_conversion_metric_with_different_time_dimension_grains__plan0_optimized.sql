test_name: test_conversion_metric_with_different_time_dimension_grains
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: Clickhouse
---
-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('month', ds) AS metric_time__month
    , user_id AS user
    , 1 AS visits
  FROM ***************************.fct_visits visits_source_src_28000
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
)

SELECT
  CAST(MAX(subq_27.buys_month) AS DOUBLE PRECISION) / CAST(NULLIF(MAX(subq_18.visits), 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate_with_monthly_conversion
FROM (
  -- Read From CTE For node_id=sma_28019
  -- Pass Only Elements: ['visits',]
  -- Aggregate Measures
  SELECT
    SUM(visits) AS visits
  FROM sma_28019_cte sma_28019_cte
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_18
CROSS JOIN
(
  -- Find conversions for user within the range of 1 month
  -- Pass Only Elements: ['buys_month',]
  -- Aggregate Measures
  SELECT
    SUM(buys_month) AS buys_month
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      FIRST_VALUE(sma_28019_cte.visits) OVER (
        PARTITION BY
          subq_23.user
          , subq_23.metric_time__month
          , subq_23.mf_internal_uuid
        ORDER BY sma_28019_cte.metric_time__month DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(sma_28019_cte.metric_time__month) OVER (
        PARTITION BY
          subq_23.user
          , subq_23.metric_time__month
          , subq_23.mf_internal_uuid
        ORDER BY sma_28019_cte.metric_time__month DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__month
      , FIRST_VALUE(sma_28019_cte.user) OVER (
        PARTITION BY
          subq_23.user
          , subq_23.metric_time__month
          , subq_23.mf_internal_uuid
        ORDER BY sma_28019_cte.metric_time__month DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , subq_23.mf_internal_uuid AS mf_internal_uuid
      , subq_23.buys_month AS buys_month
    FROM sma_28019_cte sma_28019_cte
    CROSS JOIN
    (
      -- Read Elements From Semantic Model 'buys_source'
      -- Metric Time Dimension 'ds_month'
      -- Add column with generated UUID
      SELECT
        DATE_TRUNC('month', ds_month) AS metric_time__month
        , user_id AS user
        , 1 AS buys_month
        , generateUUIDv4() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_28000
      SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
    ) subq_23
    SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
  ) subq_24
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_27
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
