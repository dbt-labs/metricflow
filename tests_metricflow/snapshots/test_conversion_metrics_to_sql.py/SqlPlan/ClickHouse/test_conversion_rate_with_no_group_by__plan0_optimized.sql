test_name: test_conversion_rate_with_no_group_by
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric with no group by data flow plan rendering.
sql_engine: ClickHouse
---
WITH sma_28019_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , user_id AS user
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  CAST(MAX(subq_33.__buys) AS Nullable(Float64)) / CAST(NULLIF(MAX(subq_22.__visits), 0) AS Nullable(Float64)) AS visit_buy_conversion_rate_7days
FROM (
  SELECT
    SUM(__visits) AS __visits
  FROM sma_28019_cte
) subq_22
CROSS JOIN (
  SELECT
    SUM(__buys) AS __buys
  FROM (
    SELECT DISTINCT
      FIRST_VALUE(sma_28019_cte.__visits) OVER (
        PARTITION BY
          subq_28.user
          , subq_28.metric_time__day
          , subq_28.mf_internal_uuid
        ORDER BY sma_28019_cte.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS __visits
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
      SELECT
        toStartOfDay(ds) AS metric_time__day
        , user_id AS user
        , 1 AS __buys
        , generateUUIDv4() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_28000
    ) subq_28
    ON
      (
        sma_28019_cte.user = subq_28.user
      ) AND (
        (
          sma_28019_cte.metric_time__day <= subq_28.metric_time__day
        ) AND (
          sma_28019_cte.metric_time__day > addDays(subq_28.metric_time__day, -7)
        )
      )
  ) subq_29
) subq_33
