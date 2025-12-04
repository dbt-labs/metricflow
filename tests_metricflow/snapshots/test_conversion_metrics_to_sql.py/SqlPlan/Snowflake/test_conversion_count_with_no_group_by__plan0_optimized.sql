test_name: test_conversion_count_with_no_group_by
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric with no group by data flow plan rendering.
sql_engine: Snowflake
---
-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
-- Write to DataTable
WITH sma_28019_cte AS (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , user_id AS user
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  COALESCE(MAX(subq_33.__buys_fill_nulls_with_0), 0) AS visit_buy_conversions
FROM (
  -- Read From CTE For node_id=sma_28019
  -- Pass Only Elements: ['__visits']
  -- Pass Only Elements: ['__visits']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    SUM(__visits) AS __visits
  FROM sma_28019_cte
) subq_22
CROSS JOIN (
  -- Find conversions for user within the range of 7 day
  -- Pass Only Elements: ['__buys_fill_nulls_with_0']
  -- Pass Only Elements: ['__buys_fill_nulls_with_0']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    SUM(__buys_fill_nulls_with_0) AS __buys_fill_nulls_with_0
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
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
      , subq_28.__buys_fill_nulls_with_0 AS __buys_fill_nulls_with_0
    FROM sma_28019_cte
    INNER JOIN (
      -- Read Elements From Semantic Model 'buys_source'
      -- Metric Time Dimension 'ds'
      -- Add column with generated UUID
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , user_id AS user
        , 1 AS __buys_fill_nulls_with_0
        , UUID_STRING() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_28000
    ) subq_28
    ON
      (
        sma_28019_cte.user = subq_28.user
      ) AND (
        (
          sma_28019_cte.metric_time__day <= subq_28.metric_time__day
        ) AND (
          sma_28019_cte.metric_time__day > DATEADD(day, -7, subq_28.metric_time__day)
        )
      )
  ) subq_29
) subq_33
