-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  CAST(MAX(subq_28.buys_month) AS FLOAT64) / CAST(NULLIF(MAX(subq_18.visits), 0) AS FLOAT64) AS visit_buy_conversion_rate_with_monthly_conversion
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
        DATETIME_TRUNC(ds, month) AS metric_time__month
        , user_id AS user
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
    ) subq_21
    INNER JOIN (
      -- Read Elements From Semantic Model 'buys_source'
      -- Metric Time Dimension 'ds_month'
      -- Add column with generated UUID
      SELECT
        DATETIME_TRUNC(ds_month, month) AS metric_time__month
        , user_id AS user
        , 1 AS buys_month
        , GENERATE_UUID() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_28000
    ) subq_24
    ON
      (
        subq_21.user = subq_24.user
      ) AND (
        (
          subq_21.metric_time__month <= subq_24.metric_time__month
        ) AND (
          subq_21.metric_time__month > DATE_SUB(CAST(subq_24.metric_time__month AS DATETIME), INTERVAL 1 month)
        )
      )
  ) subq_25
) subq_28
