-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  CAST(MAX(subq_35.buys) AS FLOAT64) / CAST(NULLIF(MAX(subq_24.visits), 0) AS FLOAT64) AS visit_buy_conversion_rate
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['visits',]
  -- Aggregate Measures
  SELECT
    SUM(visits) AS visits
  FROM (
    -- Read Elements From Semantic Model 'visits_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['visits', 'metric_time__day']
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , 1 AS visits
    FROM ***************************.fct_visits visits_source_src_28000
  ) subq_21
  WHERE metric_time__day = '2020-01-01'
) subq_24
CROSS JOIN (
  -- Find conversions for user within the range of INF
  -- Pass Only Elements: ['buys', 'metric_time__day']
  -- Aggregate Measures
  SELECT
    metric_time__day
    , SUM(buys) AS buys
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      FIRST_VALUE(subq_28.visits) OVER (
        PARTITION BY
          subq_31.user
          , subq_31.ds__day
          , subq_31.mf_internal_uuid
        ORDER BY subq_28.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(subq_28.ds__day) OVER (
        PARTITION BY
          subq_31.user
          , subq_31.ds__day
          , subq_31.mf_internal_uuid
        ORDER BY subq_28.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ds__day
      , FIRST_VALUE(subq_28.metric_time__day) OVER (
        PARTITION BY
          subq_31.user
          , subq_31.ds__day
          , subq_31.mf_internal_uuid
        ORDER BY subq_28.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__day
      , FIRST_VALUE(subq_28.user) OVER (
        PARTITION BY
          subq_31.user
          , subq_31.ds__day
          , subq_31.mf_internal_uuid
        ORDER BY subq_28.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , subq_31.mf_internal_uuid AS mf_internal_uuid
      , subq_31.buys AS buys
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['visits', 'ds__day', 'metric_time__day', 'user']
      SELECT
        ds__day
        , metric_time__day
        , subq_26.user
        , visits
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATETIME_TRUNC(ds, day) AS ds__day
          , DATETIME_TRUNC(ds, day) AS metric_time__day
          , user_id AS user
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) subq_26
      WHERE metric_time__day = '2020-01-01'
    ) subq_28
    INNER JOIN (
      -- Read Elements From Semantic Model 'buys_source'
      -- Metric Time Dimension 'ds'
      -- Add column with generated UUID
      SELECT
        DATETIME_TRUNC(ds, day) AS ds__day
        , user_id AS user
        , 1 AS buys
        , GENERATE_UUID() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_28000
    ) subq_31
    ON
      (
        subq_28.user = subq_31.user
      ) AND (
        (subq_28.ds__day <= subq_31.ds__day)
      )
  ) subq_32
  GROUP BY
    metric_time__day
) subq_35
