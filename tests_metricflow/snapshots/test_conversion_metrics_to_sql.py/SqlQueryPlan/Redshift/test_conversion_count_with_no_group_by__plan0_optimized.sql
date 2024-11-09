test_name: test_conversion_count_with_no_group_by
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric with no group by data flow plan rendering.
sql_engine: Redshift
---
-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  COALESCE(MAX(subq_28.buys), 0) AS visit_buy_conversions
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
  -- Find conversions for user within the range of 7 day
  -- Pass Only Elements: ['buys',]
  -- Aggregate Measures
  SELECT
    SUM(buys) AS buys
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      FIRST_VALUE(subq_21.visits) OVER (
        PARTITION BY
          subq_24.user
          , subq_24.metric_time__day
          , subq_24.mf_internal_uuid
        ORDER BY subq_21.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(subq_21.metric_time__day) OVER (
        PARTITION BY
          subq_24.user
          , subq_24.metric_time__day
          , subq_24.mf_internal_uuid
        ORDER BY subq_21.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__day
      , FIRST_VALUE(subq_21.user) OVER (
        PARTITION BY
          subq_24.user
          , subq_24.metric_time__day
          , subq_24.mf_internal_uuid
        ORDER BY subq_21.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , subq_24.mf_internal_uuid AS mf_internal_uuid
      , subq_24.buys AS buys
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['visits', 'metric_time__day', 'user']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , user_id AS user
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
    ) subq_21
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
    ) subq_24
    ON
      (
        subq_21.user = subq_24.user
      ) AND (
        (
          subq_21.metric_time__day <= subq_24.metric_time__day
        ) AND (
          subq_21.metric_time__day > DATEADD(day, -7, subq_24.metric_time__day)
        )
      )
  ) subq_25
) subq_28
