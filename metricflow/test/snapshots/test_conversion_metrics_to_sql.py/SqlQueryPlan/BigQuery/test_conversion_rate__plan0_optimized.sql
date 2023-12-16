-- Compute Metrics via Expressions
SELECT
  visit__referrer_id
  , CAST(buys AS FLOAT64) / CAST(NULLIF(visits, 0) AS FLOAT64) AS visit_buy_conversion_rate
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_18.visit__referrer_id, subq_28.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_18.visits) AS visits
    , MAX(subq_28.buys) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      visit__referrer_id
      , SUM(visits) AS visits
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements:
      --   ['visits', 'visit__referrer_id']
      SELECT
        referrer_id AS visit__referrer_id
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_10011
    ) subq_17
    GROUP BY
      visit__referrer_id
  ) subq_18
  FULL OUTER JOIN (
    -- Find conversions for user within the range of INF
    -- Pass Only Elements:
    --   ['buys', 'visit__referrer_id']
    -- Aggregate Measures
    SELECT
      visit__referrer_id
      , SUM(buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        first_value(subq_21.visits) OVER (PARTITION BY subq_24.user, subq_24.ds__day, subq_24.mf_internal_uuid ORDER BY subq_21.ds__day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS visits
        , first_value(subq_21.visit__referrer_id) OVER (PARTITION BY subq_24.user, subq_24.ds__day, subq_24.mf_internal_uuid ORDER BY subq_21.ds__day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS visit__referrer_id
        , first_value(subq_21.ds__day) OVER (PARTITION BY subq_24.user, subq_24.ds__day, subq_24.mf_internal_uuid ORDER BY subq_21.ds__day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ds__day
        , first_value(subq_21.user) OVER (PARTITION BY subq_24.user, subq_24.ds__day, subq_24.mf_internal_uuid ORDER BY subq_21.ds__day DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user
        , subq_24.mf_internal_uuid AS mf_internal_uuid
        , subq_24.buys AS buys
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements:
        --   ['visits', 'visit__referrer_id', 'ds__day', 'user']
        SELECT
          DATE_TRUNC(ds, day) AS ds__day
          , user_id AS user
          , referrer_id AS visit__referrer_id
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_10011
      ) subq_21
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC(ds, day) AS ds__day
          , user_id AS user
          , 1 AS buys
          , GENERATE_UUID() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_10002
      ) subq_24
      ON
        (
          subq_21.user = subq_24.user
        ) AND (
          (subq_21.ds__day <= subq_24.ds__day)
        )
    ) subq_25
    GROUP BY
      visit__referrer_id
  ) subq_28
  ON
    subq_18.visit__referrer_id = subq_28.visit__referrer_id
  GROUP BY
    visit__referrer_id
) subq_29
