-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  CAST(MAX(subq_28.buys) AS DOUBLE) / CAST(NULLIF(MAX(subq_18.visits), 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Read Elements From Semantic Model 'visits_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['visits']
  -- Aggregate Measures
  SELECT
    SUM(1) AS visits
  FROM ***************************.fct_visits visits_source_src_10011
) subq_18
CROSS JOIN (
  -- Find conversions for user within the range of 7 day
  -- Pass Only Elements:
  --   ['buys']
  -- Aggregate Measures
  SELECT
    SUM(buys) AS buys
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      first_value(subq_21.visits) OVER (PARTITION BY subq_24.user, subq_24.ds__day, subq_24.mf_internal_uuid ORDER BY subq_21.ds__day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS visits
      , first_value(subq_21.ds__day) OVER (PARTITION BY subq_24.user, subq_24.ds__day, subq_24.mf_internal_uuid ORDER BY subq_21.ds__day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ds__day
      , first_value(subq_21.user) OVER (PARTITION BY subq_24.user, subq_24.ds__day, subq_24.mf_internal_uuid ORDER BY subq_21.ds__day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user
      , subq_24.mf_internal_uuid AS mf_internal_uuid
      , subq_24.buys AS buys
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['visits', 'ds__day', 'user']
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , user_id AS user
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_10011
    ) subq_21
    INNER JOIN (
      -- Read Elements From Semantic Model 'buys_source'
      -- Metric Time Dimension 'ds'
      -- Add column with generated UUID
      SELECT
        DATE_TRUNC('day', ds) AS ds__day
        , user_id AS user
        , 1 AS buys
        , uuid() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_10002
    ) subq_24
    ON
      (
        subq_21.user = subq_24.user
      ) AND (
        (
          subq_21.ds__day <= subq_24.ds__day
        ) AND (
          subq_21.ds__day > DATE_ADD('day', -7, subq_24.ds__day)
        )
      )
  ) subq_25
) subq_28
