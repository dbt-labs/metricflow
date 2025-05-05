test_name: test_conversion_metric_with_window
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric with a window.
sql_engine: Redshift
---
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , user_id AS user
    , 1 AS visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  metric_time__day AS metric_time__day
  , CAST(buys AS DOUBLE PRECISION) / CAST(NULLIF(visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_21.metric_time__day, subq_31.metric_time__day) AS metric_time__day
    , MAX(subq_21.visits) AS visits
    , MAX(subq_31.buys) AS buys
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['visits', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(visits) AS visits
    FROM (
      -- Read From CTE For node_id=sma_28019
      SELECT
        metric_time__day
        , visits
      FROM sma_28019_cte sma_28019_cte
    ) subq_18
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__day
  ) subq_21
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['buys', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(buys) AS buys
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
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS buys
          , CONCAT(CAST(RANDOM()*100000000 AS INT)::VARCHAR,CAST(RANDOM()*100000000 AS INT)::VARCHAR) AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_27
      ON
        (
          subq_24.user = subq_27.user
        ) AND (
          (
            subq_24.metric_time__day <= subq_27.metric_time__day
          ) AND (
            subq_24.metric_time__day > DATEADD(day, -7, subq_27.metric_time__day)
          )
        )
    ) subq_28
    GROUP BY
      metric_time__day
  ) subq_31
  ON
    subq_21.metric_time__day = subq_31.metric_time__day
  GROUP BY
    COALESCE(subq_21.metric_time__day, subq_31.metric_time__day)
) subq_32
