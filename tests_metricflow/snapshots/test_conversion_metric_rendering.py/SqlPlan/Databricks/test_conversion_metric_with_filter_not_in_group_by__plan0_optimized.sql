test_name: test_conversion_metric_with_filter_not_in_group_by
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: Databricks
---
-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  COALESCE(MAX(nr_subq_26.buys), 0) AS visit_buy_conversions
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['visits',]
  -- Aggregate Measures
  SELECT
    SUM(visits) AS visits
  FROM (
    -- Read Elements From Semantic Model 'visits_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , user_id AS user
      , referrer_id AS visit__referrer_id
      , 1 AS visits
    FROM ***************************.fct_visits visits_source_src_28000
  ) nr_subq_14
  WHERE visit__referrer_id = 'ref_id_01'
) nr_subq_17
CROSS JOIN (
  -- Find conversions for user within the range of 7 day
  -- Pass Only Elements: ['buys',]
  -- Aggregate Measures
  SELECT
    SUM(buys) AS buys
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      FIRST_VALUE(nr_subq_20.visits) OVER (
        PARTITION BY
          nr_subq_22.user
          , nr_subq_22.metric_time__day
          , nr_subq_22.mf_internal_uuid
        ORDER BY nr_subq_20.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(nr_subq_20.visit__referrer_id) OVER (
        PARTITION BY
          nr_subq_22.user
          , nr_subq_22.metric_time__day
          , nr_subq_22.mf_internal_uuid
        ORDER BY nr_subq_20.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visit__referrer_id
      , FIRST_VALUE(nr_subq_20.metric_time__day) OVER (
        PARTITION BY
          nr_subq_22.user
          , nr_subq_22.metric_time__day
          , nr_subq_22.mf_internal_uuid
        ORDER BY nr_subq_20.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__day
      , FIRST_VALUE(nr_subq_20.user) OVER (
        PARTITION BY
          nr_subq_22.user
          , nr_subq_22.metric_time__day
          , nr_subq_22.mf_internal_uuid
        ORDER BY nr_subq_20.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , nr_subq_22.mf_internal_uuid AS mf_internal_uuid
      , nr_subq_22.buys AS buys
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day', 'user']
      SELECT
        metric_time__day
        , nr_subq_18.user
        , visit__referrer_id
        , visits
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , referrer_id AS visit__referrer_id
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) nr_subq_18
      WHERE visit__referrer_id = 'ref_id_01'
    ) nr_subq_20
    INNER JOIN (
      -- Read Elements From Semantic Model 'buys_source'
      -- Metric Time Dimension 'ds'
      -- Add column with generated UUID
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , user_id AS user
        , 1 AS buys
        , UUID() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_28000
    ) nr_subq_22
    ON
      (
        nr_subq_20.user = nr_subq_22.user
      ) AND (
        (
          nr_subq_20.metric_time__day <= nr_subq_22.metric_time__day
        ) AND (
          nr_subq_20.metric_time__day > DATEADD(day, -7, nr_subq_22.metric_time__day)
        )
      )
  ) nr_subq_23
) nr_subq_26
