-- Compute Metrics via Expressions
SELECT
  visit__referrer_id
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_14.visit__referrer_id, subq_22.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_14.visits) AS visits
    , MAX(subq_22.buys) AS buys
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
    ) subq_13
    GROUP BY
      visit__referrer_id
  ) subq_14
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
        FIRST_VALUE(subq_16.visits) OVER (
          PARTITION BY
            subq_19.user
            , subq_19.ds__day
            , subq_19.mf_internal_uuid
          ORDER BY subq_16.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_16.visit__referrer_id) OVER (
          PARTITION BY
            subq_19.user
            , subq_19.ds__day
            , subq_19.mf_internal_uuid
          ORDER BY subq_16.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
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
        -- Pass Only Elements: ['visits', 'visit__referrer_id', 'ds__day', 'user']
        SELECT
          DATE_TRUNC('day', ds) AS ds__day
          , user_id AS user
          , referrer_id AS visit__referrer_id
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
          (subq_16.ds__day <= subq_19.ds__day)
        )
    ) subq_20
    GROUP BY
      visit__referrer_id
  ) subq_22
  ON
    subq_14.visit__referrer_id = subq_22.visit__referrer_id
  GROUP BY
    COALESCE(subq_14.visit__referrer_id, subq_22.visit__referrer_id)
) subq_23
