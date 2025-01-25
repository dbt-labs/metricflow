test_name: test_conversion_rate
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric data flow plan rendering.
sql_engine: Redshift
---
-- Compute Metrics via Expressions
SELECT
  visit__referrer_id
  , CAST(buys AS DOUBLE PRECISION) / CAST(NULLIF(visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_2.visit__referrer_id, nr_subq_10.visit__referrer_id) AS visit__referrer_id
    , MAX(nr_subq_2.visits) AS visits
    , MAX(nr_subq_10.buys) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      visit__referrer_id
      , SUM(visits) AS visits
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['visits', 'visit__referrer_id']
      SELECT
        referrer_id AS visit__referrer_id
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
    ) nr_subq_1
    GROUP BY
      visit__referrer_id
  ) nr_subq_2
  FULL OUTER JOIN (
    -- Find conversions for user within the range of INF
    -- Pass Only Elements: ['buys', 'visit__referrer_id']
    -- Aggregate Measures
    SELECT
      visit__referrer_id
      , SUM(buys) AS buys
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
        , FIRST_VALUE(nr_subq_4.visit__referrer_id) OVER (
          PARTITION BY
            nr_subq_6.user
            , nr_subq_6.metric_time__day
            , nr_subq_6.mf_internal_uuid
          ORDER BY nr_subq_4.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
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
        -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day', 'user']
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , referrer_id AS visit__referrer_id
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) nr_subq_4
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
      ) nr_subq_6
      ON
        (
          nr_subq_4.user = nr_subq_6.user
        ) AND (
          (nr_subq_4.metric_time__day <= nr_subq_6.metric_time__day)
        )
    ) nr_subq_7
    GROUP BY
      visit__referrer_id
  ) nr_subq_10
  ON
    nr_subq_2.visit__referrer_id = nr_subq_10.visit__referrer_id
  GROUP BY
    COALESCE(nr_subq_2.visit__referrer_id, nr_subq_10.visit__referrer_id)
) nr_subq_11
