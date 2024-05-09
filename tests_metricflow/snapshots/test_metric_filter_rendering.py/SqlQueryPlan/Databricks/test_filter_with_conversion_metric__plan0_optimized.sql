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
    CAST(subq_57.buys AS DOUBLE) / CAST(NULLIF(subq_57.visits, 0) AS DOUBLE) AS user__visit_buy_conversion_rate
    , subq_42.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['listings', 'user']
    SELECT
      user_id AS user
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) subq_42
  LEFT OUTER JOIN (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(subq_46.user, subq_56.user) AS user
      , MAX(subq_46.visits) AS visits
      , MAX(subq_56.buys) AS buys
    FROM (
      -- Aggregate Measures
      SELECT
        subq_45.user
        , SUM(visits) AS visits
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['visits', 'user']
        SELECT
          user_id AS user
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) subq_45
      GROUP BY
        subq_45.user
    ) subq_46
    FULL OUTER JOIN (
      -- Find conversions for user within the range of INF
      -- Pass Only Elements: ['buys', 'user']
      -- Aggregate Measures
      SELECT
        subq_53.user
        , SUM(buys) AS buys
      FROM (
        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
        SELECT DISTINCT
          first_value(subq_49.visits) OVER (
            PARTITION BY
              subq_52.user
              , subq_52.ds__day
              , subq_52.mf_internal_uuid
            ORDER BY subq_49.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS visits
          , first_value(subq_49.ds__day) OVER (
            PARTITION BY
              subq_52.user
              , subq_52.ds__day
              , subq_52.mf_internal_uuid
            ORDER BY subq_49.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS ds__day
          , first_value(subq_49.user) OVER (
            PARTITION BY
              subq_52.user
              , subq_52.ds__day
              , subq_52.mf_internal_uuid
            ORDER BY subq_49.ds__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , subq_52.mf_internal_uuid AS mf_internal_uuid
          , subq_52.buys AS buys
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['visits', 'ds__day', 'user']
          SELECT
            DATE_TRUNC('day', ds) AS ds__day
            , user_id AS user
            , 1 AS visits
          FROM ***************************.fct_visits visits_source_src_28000
        ) subq_49
        INNER JOIN (
          -- Read Elements From Semantic Model 'buys_source'
          -- Metric Time Dimension 'ds'
          -- Add column with generated UUID
          SELECT
            DATE_TRUNC('day', ds) AS ds__day
            , user_id AS user
            , 1 AS buys
            , UUID() AS mf_internal_uuid
          FROM ***************************.fct_buys buys_source_src_28000
        ) subq_52
        ON
          (
            subq_49.user = subq_52.user
          ) AND (
            (subq_49.ds__day <= subq_52.ds__day)
          )
      ) subq_53
      GROUP BY
        subq_53.user
    ) subq_56
    ON
      subq_46.user = subq_56.user
    GROUP BY
      COALESCE(subq_46.user, subq_56.user)
  ) subq_57
  ON
    subq_42.user = subq_57.user
) subq_61
WHERE user__visit_buy_conversion_rate > 2
