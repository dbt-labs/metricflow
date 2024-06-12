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
    CAST(subq_72.buys AS DOUBLE) / CAST(NULLIF(subq_72.visits, 0) AS DOUBLE) AS user__visit_buy_conversion_rate
    , subq_57.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'user']
    SELECT
      user_id AS user
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_57
  LEFT OUTER JOIN (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_61.user, subq_71.user) AS user
      , MAX(subq_61.visits) AS visits
      , MAX(subq_71.buys) AS buys
    FROM (
      -- Aggregate Measures
      SELECT
        subq_60.user
        , SUM(visits) AS visits
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['visits', 'user']
        SELECT
          user_id AS user
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) subq_60
      GROUP BY
        subq_60.user
    ) subq_61
    FULL OUTER JOIN (
      -- Find conversions for user within the range of INF
      -- Pass Only Elements: ['buys', 'user']
      -- Aggregate Measures
      SELECT
        subq_68.user
        , SUM(buys) AS buys
      FROM (
        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
        SELECT DISTINCT
          FIRST_VALUE(subq_64.visits) OVER (
            PARTITION BY
              subq_67.user
              , subq_67.ds__day
              , subq_67.mf_internal_uuid
            ORDER BY subq_64.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS visits
          , FIRST_VALUE(subq_64.ds__day) OVER (
            PARTITION BY
              subq_67.user
              , subq_67.ds__day
              , subq_67.mf_internal_uuid
            ORDER BY subq_64.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS ds__day
          , FIRST_VALUE(subq_64.user) OVER (
            PARTITION BY
              subq_67.user
              , subq_67.ds__day
              , subq_67.mf_internal_uuid
            ORDER BY subq_64.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , subq_67.mf_internal_uuid AS mf_internal_uuid
          , subq_67.buys AS buys
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['visits', 'ds__day', 'user']
          SELECT
            DATE_TRUNC('day', ds) AS ds__day
            , user_id AS user
            , 1 AS visits
          FROM ***************************.fct_visits visits_source_src_28000
        ) subq_64
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
        ) subq_67
        ON
          (
            subq_64.user = subq_67.user
          ) AND (
            (subq_64.ds__day <= subq_67.ds__day)
          )
      ) subq_68
      GROUP BY
        subq_68.user
    ) subq_71
    ON
      subq_61.user = subq_71.user
    GROUP BY
      COALESCE(subq_61.user, subq_71.user)
  ) subq_72
  ON
    subq_57.user = subq_72.user
) subq_76
WHERE user__visit_buy_conversion_rate > 2
