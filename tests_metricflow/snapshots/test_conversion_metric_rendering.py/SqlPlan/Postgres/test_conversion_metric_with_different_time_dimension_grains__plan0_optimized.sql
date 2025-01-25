test_name: test_conversion_metric_with_different_time_dimension_grains
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: Postgres
---
-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  CAST(MAX(nr_subq_22.buys_month) AS DOUBLE PRECISION) / CAST(NULLIF(MAX(nr_subq_14.visits), 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate_with_monthly_conversion
FROM (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['visits',]
  -- Aggregate Measures
  SELECT
    SUM(1) AS visits
  FROM ***************************.fct_visits visits_source_src_28000
) nr_subq_14
CROSS JOIN (
  -- Find conversions for user within the range of 1 month
  -- Pass Only Elements: ['buys_month',]
  -- Aggregate Measures
  SELECT
    SUM(buys_month) AS buys_month
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      FIRST_VALUE(nr_subq_16.visits) OVER (
        PARTITION BY
          nr_subq_18.user
          , nr_subq_18.metric_time__month
          , nr_subq_18.mf_internal_uuid
        ORDER BY nr_subq_16.metric_time__month DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(nr_subq_16.metric_time__month) OVER (
        PARTITION BY
          nr_subq_18.user
          , nr_subq_18.metric_time__month
          , nr_subq_18.mf_internal_uuid
        ORDER BY nr_subq_16.metric_time__month DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__month
      , FIRST_VALUE(nr_subq_16.user) OVER (
        PARTITION BY
          nr_subq_18.user
          , nr_subq_18.metric_time__month
          , nr_subq_18.mf_internal_uuid
        ORDER BY nr_subq_16.metric_time__month DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , nr_subq_18.mf_internal_uuid AS mf_internal_uuid
      , nr_subq_18.buys_month AS buys_month
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['visits', 'metric_time__month', 'user']
      SELECT
        DATE_TRUNC('month', ds) AS metric_time__month
        , user_id AS user
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
    ) nr_subq_16
    INNER JOIN (
      -- Read Elements From Semantic Model 'buys_source'
      -- Metric Time Dimension 'ds_month'
      -- Add column with generated UUID
      SELECT
        DATE_TRUNC('month', ds_month) AS metric_time__month
        , user_id AS user
        , 1 AS buys_month
        , GEN_RANDOM_UUID() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_28000
    ) nr_subq_18
    ON
      (
        nr_subq_16.user = nr_subq_18.user
      ) AND (
        (
          nr_subq_16.metric_time__month <= nr_subq_18.metric_time__month
        ) AND (
          nr_subq_16.metric_time__month > nr_subq_18.metric_time__month - MAKE_INTERVAL(months => 1)
        )
      )
  ) nr_subq_19
) nr_subq_22
