-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  COALESCE(MAX(subq_22.buys), 0) AS visit_buy_conversions
FROM (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['visits',]
  -- Aggregate Measures
  SELECT
    SUM(1) AS visits
  FROM ***************************.fct_visits visits_source_src_28000
) subq_14
CROSS JOIN (
  -- Find conversions for user within the range of 7 day
  -- Pass Only Elements: ['buys',]
  -- Aggregate Measures
  SELECT
    SUM(buys) AS buys
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      FIRST_VALUE(subq_16.visits) OVER (
        PARTITION BY
          subq_19.user
          , subq_19.ds__day
          , subq_19.mf_internal_uuid
        ORDER BY subq_16.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(subq_16.ds__day) OVER (
        PARTITION BY
          subq_19.user
          , subq_19.ds__day
          , subq_19.mf_internal_uuid
        ORDER BY subq_16.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ds__day
      , FIRST_VALUE(subq_16.user) OVER (
        PARTITION BY
          subq_19.user
          , subq_19.ds__day
          , subq_19.mf_internal_uuid
        ORDER BY subq_16.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , subq_19.mf_internal_uuid AS mf_internal_uuid
      , subq_19.buys AS buys
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['visits', 'ds__day', 'user']
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , user_id AS user
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
    ) subq_16
    INNER JOIN (
      -- Read Elements From Semantic Model 'buys_source'
      -- Metric Time Dimension 'ds'
      -- Add column with generated UUID
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , user_id AS user
        , 1 AS buys
        , GEN_RANDOM_UUID() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_28000
    ) subq_19
    ON
      (
        subq_16.user = subq_19.user
      ) AND (
        (
          subq_16.ds__day <= subq_19.ds__day
        ) AND (
          subq_16.ds__day > subq_19.ds__day - INTERVAL 7 day
        )
      )
  ) subq_20
) subq_22
