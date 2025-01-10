test_name: test_conversion_rate_with_window
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric with a window data flow plan rendering.
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , user_id AS user
    , referrer_id AS visit__referrer_id
    , 1 AS visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , visit__referrer_id AS visit__referrer_id
  , CAST(buys AS FLOAT64) / CAST(NULLIF(visits, 0) AS FLOAT64) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_18.metric_time__day, subq_27.metric_time__day) AS metric_time__day
    , COALESCE(subq_18.visit__referrer_id, subq_27.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_18.visits) AS visits
    , MAX(subq_27.buys) AS buys
  FROM (
    -- Read From CTE For node_id=sma_28019
    -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , visit__referrer_id
      , SUM(visits) AS visits
    FROM sma_28019_cte sma_28019_cte
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) subq_18
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['buys', 'visit__referrer_id', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , visit__referrer_id
      , SUM(buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(sma_28019_cte.visits) OVER (
          PARTITION BY
            subq_23.user
            , subq_23.metric_time__day
            , subq_23.mf_internal_uuid
          ORDER BY sma_28019_cte.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(sma_28019_cte.visit__referrer_id) OVER (
          PARTITION BY
            subq_23.user
            , subq_23.metric_time__day
            , subq_23.mf_internal_uuid
          ORDER BY sma_28019_cte.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(sma_28019_cte.metric_time__day) OVER (
          PARTITION BY
            subq_23.user
            , subq_23.metric_time__day
            , subq_23.mf_internal_uuid
          ORDER BY sma_28019_cte.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(sma_28019_cte.user) OVER (
          PARTITION BY
            subq_23.user
            , subq_23.metric_time__day
            , subq_23.mf_internal_uuid
          ORDER BY sma_28019_cte.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_23.mf_internal_uuid AS mf_internal_uuid
        , subq_23.buys AS buys
      FROM sma_28019_cte sma_28019_cte
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
      ) subq_23
      ON
        (
          sma_28019_cte.user = subq_23.user
        ) AND (
          (
            sma_28019_cte.metric_time__day <= subq_23.metric_time__day
          ) AND (
            sma_28019_cte.metric_time__day > DATE_SUB(CAST(subq_23.metric_time__day AS DATETIME), INTERVAL 7 day)
          )
        )
    ) subq_24
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) subq_27
  ON
    (
      subq_18.visit__referrer_id = subq_27.visit__referrer_id
    ) AND (
      subq_18.metric_time__day = subq_27.metric_time__day
    )
  GROUP BY
    metric_time__day
    , visit__referrer_id
) subq_28