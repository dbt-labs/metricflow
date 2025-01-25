test_name: test_conversion_metric_with_window_and_time_constraint
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric with a window, time constraint, and categorical filter.
sql_engine: Databricks
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , visit__referrer_id
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(nr_subq_19.metric_time__day, nr_subq_28.metric_time__day) AS metric_time__day
    , COALESCE(nr_subq_19.visit__referrer_id, nr_subq_28.visit__referrer_id) AS visit__referrer_id
    , MAX(nr_subq_19.visits) AS visits
    , MAX(nr_subq_28.buys) AS buys
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
      -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , user_id AS user
        , referrer_id AS visit__referrer_id
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
      WHERE DATE_TRUNC('day', ds) BETWEEN '2020-01-01' AND '2020-01-02'
    ) nr_subq_16
    WHERE visit__referrer_id = 'ref_id_01'
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) nr_subq_19
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
        FIRST_VALUE(nr_subq_22.visits) OVER (
          PARTITION BY
            nr_subq_24.user
            , nr_subq_24.metric_time__day
            , nr_subq_24.mf_internal_uuid
          ORDER BY nr_subq_22.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(nr_subq_22.visit__referrer_id) OVER (
          PARTITION BY
            nr_subq_24.user
            , nr_subq_24.metric_time__day
            , nr_subq_24.mf_internal_uuid
          ORDER BY nr_subq_22.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(nr_subq_22.metric_time__day) OVER (
          PARTITION BY
            nr_subq_24.user
            , nr_subq_24.metric_time__day
            , nr_subq_24.mf_internal_uuid
          ORDER BY nr_subq_22.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(nr_subq_22.user) OVER (
          PARTITION BY
            nr_subq_24.user
            , nr_subq_24.metric_time__day
            , nr_subq_24.mf_internal_uuid
          ORDER BY nr_subq_22.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , nr_subq_24.mf_internal_uuid AS mf_internal_uuid
        , nr_subq_24.buys AS buys
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day', 'user']
        SELECT
          metric_time__day
          , nr_subq_20.user
          , visit__referrer_id
          , visits
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          -- Metric Time Dimension 'ds'
          -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , user_id AS user
            , referrer_id AS visit__referrer_id
            , 1 AS visits
          FROM ***************************.fct_visits visits_source_src_28000
          WHERE DATE_TRUNC('day', ds) BETWEEN '2020-01-01' AND '2020-01-02'
        ) nr_subq_20
        WHERE visit__referrer_id = 'ref_id_01'
      ) nr_subq_22
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
      ) nr_subq_24
      ON
        (
          nr_subq_22.user = nr_subq_24.user
        ) AND (
          (
            nr_subq_22.metric_time__day <= nr_subq_24.metric_time__day
          ) AND (
            nr_subq_22.metric_time__day > DATEADD(day, -7, nr_subq_24.metric_time__day)
          )
        )
    ) nr_subq_25
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) nr_subq_28
  ON
    (
      nr_subq_19.visit__referrer_id = nr_subq_28.visit__referrer_id
    ) AND (
      nr_subq_19.metric_time__day = nr_subq_28.metric_time__day
    )
  GROUP BY
    COALESCE(nr_subq_19.metric_time__day, nr_subq_28.metric_time__day)
    , COALESCE(nr_subq_19.visit__referrer_id, nr_subq_28.visit__referrer_id)
) nr_subq_29
