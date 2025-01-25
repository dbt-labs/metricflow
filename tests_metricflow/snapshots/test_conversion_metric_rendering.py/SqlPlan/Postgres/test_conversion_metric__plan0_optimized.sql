test_name: test_conversion_metric
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: Postgres
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , CAST(buys AS DOUBLE PRECISION) / CAST(NULLIF(visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_17.metric_time__day, nr_subq_26.metric_time__day) AS metric_time__day
    , MAX(nr_subq_17.visits) AS visits
    , MAX(nr_subq_26.buys) AS buys
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['visits', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(visits) AS visits
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , user_id AS user
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
    ) nr_subq_14
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__day
  ) nr_subq_17
  FULL OUTER JOIN (
    -- Find conversions for user within the range of INF
    -- Pass Only Elements: ['buys', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(buys) AS buys
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
        -- Pass Only Elements: ['visits', 'metric_time__day', 'user']
        SELECT
          metric_time__day
          , nr_subq_18.user
          , visits
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , user_id AS user
            , 1 AS visits
          FROM ***************************.fct_visits visits_source_src_28000
        ) nr_subq_18
        WHERE metric_time__day = '2020-01-01'
      ) nr_subq_20
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS buys
          , GEN_RANDOM_UUID() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) nr_subq_22
      ON
        (
          nr_subq_20.user = nr_subq_22.user
        ) AND (
          (nr_subq_20.metric_time__day <= nr_subq_22.metric_time__day)
        )
    ) nr_subq_23
    GROUP BY
      metric_time__day
  ) nr_subq_26
  ON
    nr_subq_17.metric_time__day = nr_subq_26.metric_time__day
  GROUP BY
    COALESCE(nr_subq_17.metric_time__day, nr_subq_26.metric_time__day)
) nr_subq_27
