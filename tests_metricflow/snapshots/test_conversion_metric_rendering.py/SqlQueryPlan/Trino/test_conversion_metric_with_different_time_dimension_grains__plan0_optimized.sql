test_name: test_conversion_metric_with_different_time_dimension_grains
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
---
-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  CAST(MAX(subq_28.buys_month) AS DOUBLE) / CAST(NULLIF(MAX(subq_18.visits), 0) AS DOUBLE) AS visit_buy_conversion_rate_with_monthly_conversion
FROM (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['visits',]
  -- Aggregate Measures
  SELECT
    SUM(1) AS visits
  FROM ***************************.fct_visits visits_source_src_28000
) subq_18
CROSS JOIN (
  -- Find conversions for user within the range of 1 month
  -- Pass Only Elements: ['buys_month',]
  -- Aggregate Measures
  SELECT
    SUM(buys_month) AS buys_month
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      FIRST_VALUE(subq_21.visits) OVER (
        PARTITION BY
          subq_24.user
          , subq_24.metric_time__month
          , subq_24.mf_internal_uuid
        ORDER BY subq_21.metric_time__month DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(subq_21.metric_time__month) OVER (
        PARTITION BY
          subq_24.user
          , subq_24.metric_time__month
          , subq_24.mf_internal_uuid
        ORDER BY subq_21.metric_time__month DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__month
      , FIRST_VALUE(subq_21.user) OVER (
        PARTITION BY
          subq_24.user
          , subq_24.metric_time__month
          , subq_24.mf_internal_uuid
        ORDER BY subq_21.metric_time__month DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , subq_24.mf_internal_uuid AS mf_internal_uuid
      , subq_24.buys_month AS buys_month
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['visits', 'metric_time__month', 'user']
      SELECT
        DATE_TRUNC('month', ds) AS metric_time__month
        , user_id AS user
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
    ) subq_21
    INNER JOIN (
      -- Read Elements From Semantic Model 'buys_source'
      -- Metric Time Dimension 'ds_month'
      -- Add column with generated UUID
      SELECT
        DATE_TRUNC('month', ds_month) AS metric_time__month
        , user_id AS user
        , 1 AS buys_month
        , uuid() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_28000
    ) subq_24
    ON
      (
        subq_21.user = subq_24.user
      ) AND (
        (
          subq_21.metric_time__month <= subq_24.metric_time__month
        ) AND (
          subq_21.metric_time__month > DATE_ADD('month', -1, subq_24.metric_time__month)
        )
      )
  ) subq_25
) subq_28
