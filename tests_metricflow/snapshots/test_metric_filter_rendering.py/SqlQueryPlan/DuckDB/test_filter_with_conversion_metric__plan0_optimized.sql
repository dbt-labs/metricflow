-- Constrain Output with WHERE
-- Pass Only Elements: ['listings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements: ['listings', 'user__visit_buy_conversion_rate']
  SELECT
    CAST(subq_31.buys AS DOUBLE) / CAST(NULLIF(subq_31.visits, 0) AS DOUBLE) AS user__visit_buy_conversion_rate
    , subq_19.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'user']
    SELECT
      user_id AS user
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_19
  LEFT OUTER JOIN (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_22.user, subq_30.user) AS user
      , MAX(subq_22.visits) AS visits
      , MAX(subq_30.buys) AS buys
    FROM (
      -- Aggregate Measures
      SELECT
        subq_21.user
        , SUM(visits) AS visits
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['visits', 'user']
        SELECT
          user_id AS user
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) subq_21
      GROUP BY
        subq_21.user
    ) subq_22
    FULL OUTER JOIN (
      -- Find conversions for user within the range of INF
      -- Pass Only Elements: ['buys', 'user']
      -- Aggregate Measures
      SELECT
        subq_28.user
        , SUM(buys) AS buys
      FROM (
        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
        SELECT DISTINCT
          FIRST_VALUE(subq_24.visits) OVER (
            PARTITION BY
              subq_27.user
              , subq_27.ds__day
              , subq_27.mf_internal_uuid
            ORDER BY subq_24.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS visits
          , FIRST_VALUE(subq_24.ds__day) OVER (
            PARTITION BY
              subq_27.user
              , subq_27.ds__day
              , subq_27.mf_internal_uuid
            ORDER BY subq_24.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS ds__day
          , FIRST_VALUE(subq_24.user) OVER (
            PARTITION BY
              subq_27.user
              , subq_27.ds__day
              , subq_27.mf_internal_uuid
            ORDER BY subq_24.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , subq_27.mf_internal_uuid AS mf_internal_uuid
          , subq_27.buys AS buys
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['visits', 'ds__day', 'user']
          SELECT
            DATE_TRUNC('day', ds) AS ds__day
            , user_id AS user
            , 1 AS visits
          FROM ***************************.fct_visits visits_source_src_28000
        ) subq_24
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
        ) subq_27
        ON
          (
            subq_24.user = subq_27.user
          ) AND (
            (subq_24.ds__day <= subq_27.ds__day)
          )
      ) subq_28
      GROUP BY
        subq_28.user
    ) subq_30
    ON
      subq_22.user = subq_30.user
    GROUP BY
      COALESCE(subq_22.user, subq_30.user)
  ) subq_31
  ON
    subq_19.user = subq_31.user
) subq_33
WHERE user__visit_buy_conversion_rate > 2
