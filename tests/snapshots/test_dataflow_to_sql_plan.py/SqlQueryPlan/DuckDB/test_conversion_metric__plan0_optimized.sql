-- Compute Metrics via Expressions
SELECT
  visit__referrer_id
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_19.visit__referrer_id, subq_30.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_19.visits) AS visits
    , MAX(subq_30.buys) AS buys
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
    ) subq_18
    GROUP BY
      visit__referrer_id
  ) subq_19
  FULL OUTER JOIN (
    -- Find conversions for EntitySpec(element_name='user', entity_links=()) within the range of count=7 granularity=TimeGranularity.DAY
    -- Pass Only Elements:
    --   ['buys', 'visit__referrer_id']
    -- Aggregate Measures
    SELECT
      visit__referrer_id
      , SUM(buys) AS buys
    FROM (
      -- Dedupe the fanout on (MetadataSpec(element_name='mf_internal_uuid'),) in the conversion data set
      SELECT DISTINCT
        first_value(subq_22.visits) OVER (PARTITION BY subq_26.user, subq_26.ds__day ORDER BY subq_22.ds__day DESC) AS visits
        , first_value(subq_22.visit__referrer_id) OVER (PARTITION BY subq_26.user, subq_26.ds__day ORDER BY subq_22.ds__day DESC) AS visit__referrer_id
        , subq_26.mf_internal_uuid AS mf_internal_uuid
        , subq_26.buys AS buys
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements:
        --   ['visits', 'visit__referrer_id']
        SELECT
          referrer_id AS visit__referrer_id
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_10011
      ) subq_22
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        -- Pass Only Elements:
        --   ['buys', 'mf_internal_uuid']
        SELECT
          1 AS buys
          , GEN_RANDOM_UUID() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_10002
      ) subq_26
      ON
        (
          subq_22.user = subq_26.user
        ) AND (
          (
            subq_22.ds__day <= subq_26.ds__day
          ) AND (
            subq_22.ds__day > subq_26.ds__day - INTERVAL 7 day
          )
        )
    ) subq_27
    GROUP BY
      visit__referrer_id
  ) subq_30
  ON
    subq_19.visit__referrer_id = subq_30.visit__referrer_id
  GROUP BY
    COALESCE(subq_19.visit__referrer_id, subq_30.visit__referrer_id)
) subq_31
