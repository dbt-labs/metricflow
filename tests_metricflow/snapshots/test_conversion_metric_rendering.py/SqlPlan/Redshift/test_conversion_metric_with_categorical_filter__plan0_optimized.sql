test_name: test_conversion_metric_with_categorical_filter
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric with a categorical filter.
sql_engine: Redshift
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , visit__referrer_id
  , CAST(buys AS DOUBLE PRECISION) / CAST(NULLIF(visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_17.metric_time__day, nr_subq_26.metric_time__day) AS metric_time__day
    , COALESCE(nr_subq_17.visit__referrer_id, nr_subq_26.visit__referrer_id) AS visit__referrer_id
    , MAX(nr_subq_17.visits) AS visits
    , MAX(nr_subq_26.buys) AS buys
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , visit__referrer_id
      , SUM(visits) AS visits
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
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) nr_subq_17
  FULL OUTER JOIN (
    -- Find conversions for user within the range of INF
    -- Pass Only Elements: ['buys', 'visit__referrer_id', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , visit__referrer_id
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
          , CONCAT(CAST(RANDOM()*100000000 AS INT)::VARCHAR,CAST(RANDOM()*100000000 AS INT)::VARCHAR) AS mf_internal_uuid
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
      , visit__referrer_id
  ) nr_subq_26
  ON
    (
      nr_subq_17.visit__referrer_id = nr_subq_26.visit__referrer_id
    ) AND (
      nr_subq_17.metric_time__day = nr_subq_26.metric_time__day
    )
  GROUP BY
    COALESCE(nr_subq_17.metric_time__day, nr_subq_26.metric_time__day)
    , COALESCE(nr_subq_17.visit__referrer_id, nr_subq_26.visit__referrer_id)
) nr_subq_27
