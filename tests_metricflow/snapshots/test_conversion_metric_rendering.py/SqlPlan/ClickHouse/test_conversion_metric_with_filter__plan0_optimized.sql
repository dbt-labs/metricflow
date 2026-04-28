test_name: test_conversion_metric_with_filter
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
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
  CAST(MAX(subq_37.__buys) AS Nullable(Float64)) / CAST(NULLIF(MAX(subq_25.__visits), 0) AS Nullable(Float64)) AS visit_buy_conversion_rate
FROM (
  SELECT
    SUM(__visits) AS __visits
  FROM (
    SELECT
      visits AS __visits
    FROM (
      SELECT
        metric_time__day
        , __visits AS visits
      FROM sma_28019_cte
    ) subq_22
    WHERE metric_time__day = '2020-01-01'
  ) subq_24
) subq_25
CROSS JOIN (
  SELECT
    SUM(__buys) AS __buys
  FROM (
    SELECT DISTINCT
      FIRST_VALUE(subq_29.__visits) OVER (
        PARTITION BY
          subq_32.user
          , subq_32.metric_time__day
          , subq_32.mf_internal_uuid
        ORDER BY subq_29.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS __visits
      , FIRST_VALUE(subq_29.metric_time__day) OVER (
        PARTITION BY
          subq_32.user
          , subq_32.metric_time__day
          , subq_32.mf_internal_uuid
        ORDER BY subq_29.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__day
      , FIRST_VALUE(subq_29.user) OVER (
        PARTITION BY
          subq_32.user
          , subq_32.metric_time__day
          , subq_32.mf_internal_uuid
        ORDER BY subq_29.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , subq_32.mf_internal_uuid AS mf_internal_uuid
      , subq_32.__buys AS __buys
    FROM (
      SELECT
        metric_time__day
        , subq_27.user
        , visits AS __visits
      FROM (
        SELECT
          metric_time__day
          , sma_28019_cte.user
          , __visits AS visits
        FROM sma_28019_cte
      ) subq_27
      WHERE metric_time__day = '2020-01-01'
    ) subq_29
    INNER JOIN (
      SELECT
        toStartOfDay(ds) AS metric_time__day
        , user_id AS user
        , 1 AS __buys
        , generateUUIDv4() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_28000
    ) subq_32
    ON
      (
        subq_29.user = subq_32.user
      ) AND (
        (subq_29.metric_time__day <= subq_32.metric_time__day)
      )
  ) subq_33
) subq_37
