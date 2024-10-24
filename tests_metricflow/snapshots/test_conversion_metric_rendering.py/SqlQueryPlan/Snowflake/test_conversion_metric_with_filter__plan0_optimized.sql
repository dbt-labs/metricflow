-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  CAST(MAX(subq_34.buys) AS DOUBLE) / CAST(NULLIF(MAX(subq_23.visits), 0) AS DOUBLE) AS visit_buy_conversion_rate
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
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS visits
    FROM ***************************.fct_visits visits_source_src_28000
  ) subq_20
  WHERE metric_time__day = '2020-01-01'
) subq_23
CROSS JOIN (
  -- Find conversions for user within the range of INF
  -- Pass Only Elements: ['buys',]
  -- Aggregate Measures
  SELECT
    SUM(buys) AS buys
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      FIRST_VALUE(subq_27.visits) OVER (
        PARTITION BY
          subq_30.user
          , subq_30.ds__day
          , subq_30.mf_internal_uuid
        ORDER BY subq_27.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(subq_27.ds__day) OVER (
        PARTITION BY
          subq_30.user
          , subq_30.ds__day
          , subq_30.mf_internal_uuid
        ORDER BY subq_27.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ds__day
      , FIRST_VALUE(subq_27.metric_time__day) OVER (
        PARTITION BY
          subq_30.user
          , subq_30.ds__day
          , subq_30.mf_internal_uuid
        ORDER BY subq_27.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__day
      , FIRST_VALUE(subq_27.user) OVER (
        PARTITION BY
          subq_30.user
          , subq_30.ds__day
          , subq_30.mf_internal_uuid
        ORDER BY subq_27.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , subq_30.mf_internal_uuid AS mf_internal_uuid
      , subq_30.buys AS buys
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['visits', 'ds__day', 'metric_time__day', 'user']
      SELECT
        ds__day
        , metric_time__day
        , subq_25.user
        , visits
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS ds__day
          , DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) subq_25
      WHERE metric_time__day = '2020-01-01'
    ) subq_27
    INNER JOIN (
      -- Read Elements From Semantic Model 'buys_source'
      -- Metric Time Dimension 'ds'
      -- Add column with generated UUID
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , user_id AS user
        , 1 AS buys
        , UUID_STRING() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_28000
    ) subq_30
    ON
      (
        subq_27.user = subq_30.user
      ) AND (
        (subq_27.ds__day <= subq_30.ds__day)
      )
  ) subq_31
) subq_34
