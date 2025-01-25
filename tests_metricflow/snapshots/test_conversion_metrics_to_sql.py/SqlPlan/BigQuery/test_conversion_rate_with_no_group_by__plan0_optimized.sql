test_name: test_conversion_rate_with_no_group_by
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric with no group by data flow plan rendering.
sql_engine: BigQuery
---
-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  CAST(MAX(nr_subq_10.buys) AS FLOAT64) / CAST(NULLIF(MAX(nr_subq_2.visits), 0) AS FLOAT64) AS visit_buy_conversion_rate_7days
FROM (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['visits',]
  -- Aggregate Measures
  SELECT
    SUM(1) AS visits
  FROM ***************************.fct_visits visits_source_src_28000
) nr_subq_2
CROSS JOIN (
  -- Find conversions for user within the range of 7 day
  -- Pass Only Elements: ['buys',]
  -- Aggregate Measures
  SELECT
    SUM(buys) AS buys
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      FIRST_VALUE(nr_subq_4.visits) OVER (
        PARTITION BY
          nr_subq_6.user
          , nr_subq_6.metric_time__day
          , nr_subq_6.mf_internal_uuid
        ORDER BY nr_subq_4.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(nr_subq_4.metric_time__day) OVER (
        PARTITION BY
          nr_subq_6.user
          , nr_subq_6.metric_time__day
          , nr_subq_6.mf_internal_uuid
        ORDER BY nr_subq_4.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__day
      , FIRST_VALUE(nr_subq_4.user) OVER (
        PARTITION BY
          nr_subq_6.user
          , nr_subq_6.metric_time__day
          , nr_subq_6.mf_internal_uuid
        ORDER BY nr_subq_4.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , nr_subq_6.mf_internal_uuid AS mf_internal_uuid
      , nr_subq_6.buys AS buys
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['visits', 'metric_time__day', 'user']
      SELECT
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , user_id AS user
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
    ) nr_subq_4
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
    ) nr_subq_6
    ON
      (
        nr_subq_4.user = nr_subq_6.user
      ) AND (
        (
          nr_subq_4.metric_time__day <= nr_subq_6.metric_time__day
        ) AND (
          nr_subq_4.metric_time__day > DATE_SUB(CAST(nr_subq_6.metric_time__day AS DATETIME), INTERVAL 7 day)
        )
      )
  ) nr_subq_7
) nr_subq_10
