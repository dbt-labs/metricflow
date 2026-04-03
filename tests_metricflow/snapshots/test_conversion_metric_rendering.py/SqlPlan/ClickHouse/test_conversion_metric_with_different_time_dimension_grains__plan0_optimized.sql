test_name: test_conversion_metric_with_different_time_dimension_grains
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: ClickHouse
---
WITH sma_28019_cte AS (
  SELECT
    toStartOfMonth(ds) AS metric_time__month
    , user_id AS user
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  CAST(MAX(subq_33.__buys_month) AS Nullable(Float64)) / CAST(NULLIF(MAX(subq_22.__visits), 0) AS Nullable(Float64)) AS visit_buy_conversion_rate_with_monthly_conversion
FROM (
  SELECT
    SUM(__visits) AS __visits
  FROM sma_28019_cte
) subq_22
CROSS JOIN (
  SELECT
    SUM(__buys_month) AS __buys_month
  FROM (
    SELECT DISTINCT
      FIRST_VALUE(sma_28019_cte.__visits) OVER (
        PARTITION BY
          subq_28.user
          , subq_28.metric_time__month
          , subq_28.mf_internal_uuid
        ORDER BY sma_28019_cte.metric_time__month DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS __visits
      , FIRST_VALUE(sma_28019_cte.metric_time__month) OVER (
        PARTITION BY
          subq_28.user
          , subq_28.metric_time__month
          , subq_28.mf_internal_uuid
        ORDER BY sma_28019_cte.metric_time__month DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__month
      , FIRST_VALUE(sma_28019_cte.user) OVER (
        PARTITION BY
          subq_28.user
          , subq_28.metric_time__month
          , subq_28.mf_internal_uuid
        ORDER BY sma_28019_cte.metric_time__month DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , subq_28.mf_internal_uuid AS mf_internal_uuid
      , subq_28.__buys_month AS __buys_month
    FROM sma_28019_cte
    INNER JOIN (
      SELECT
        toStartOfMonth(ds_month) AS metric_time__month
        , user_id AS user
        , 1 AS __buys_month
        , generateUUIDv4() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_28000
    ) subq_28
    ON
      (
        sma_28019_cte.user = subq_28.user
      ) AND (
        (
          sma_28019_cte.metric_time__month <= subq_28.metric_time__month
        ) AND (
          sma_28019_cte.metric_time__month > addMonths(subq_28.metric_time__month, -1)
        )
      )
  ) subq_29
) subq_33
