-- Compute Metrics via Expressions
SELECT
  visit__referrer_id
  , CAST(buys AS FLOAT64) / CAST(NULLIF(visits, 0) AS FLOAT64) AS visit_buy_conversion_rate
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_24.visit__referrer_id, subq_36.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_24.visits) AS visits
    , MAX(subq_36.buys) AS buys
  FROM (
    -- Constrain Output with WHERE
    -- Aggregate Measures
    SELECT
      visit__referrer_id
      , SUM(visits) AS visits
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
      -- Pass Only Elements: ['visits', 'visit__referrer_id']
      SELECT
        referrer_id AS visit__referrer_id
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
      WHERE DATETIME_TRUNC(ds, day) BETWEEN '2020-01-01' AND '2020-01-02'
    ) subq_22
    WHERE visit__referrer_id = 'ref_id_01'
    GROUP BY
      visit__referrer_id
  ) subq_24
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
        FIRST_VALUE(subq_29.visits) OVER (
          PARTITION BY
            subq_32.user
            , subq_32.ds__day
            , subq_32.mf_internal_uuid
          ORDER BY subq_29.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_29.visit__referrer_id) OVER (
          PARTITION BY
            subq_32.user
            , subq_32.ds__day
            , subq_32.mf_internal_uuid
          ORDER BY subq_29.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(subq_29.ds__day) OVER (
          PARTITION BY
            subq_32.user
            , subq_32.ds__day
            , subq_32.mf_internal_uuid
          ORDER BY subq_29.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__day
        , FIRST_VALUE(subq_29.user) OVER (
          PARTITION BY
            subq_32.user
            , subq_32.ds__day
            , subq_32.mf_internal_uuid
          ORDER BY subq_29.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_32.mf_internal_uuid AS mf_internal_uuid
        , subq_32.buys AS buys
      FROM (
        -- Constrain Output with WHERE
        -- Pass Only Elements: ['visits', 'visit__referrer_id', 'ds__day', 'user']
        SELECT
          ds__day
          , subq_27.user
          , visit__referrer_id
          , visits
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          -- Metric Time Dimension 'ds'
          -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
          SELECT
            DATETIME_TRUNC(ds, day) AS ds__day
            , user_id AS user
            , referrer_id AS visit__referrer_id
            , 1 AS visits
          FROM ***************************.fct_visits visits_source_src_28000
          WHERE DATETIME_TRUNC(ds, day) BETWEEN '2020-01-01' AND '2020-01-02'
        ) subq_27
        WHERE visit__referrer_id = 'ref_id_01'
      ) subq_29
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
      ) subq_32
      ON
        (
          subq_29.user = subq_32.user
        ) AND (
          (subq_29.ds__day <= subq_32.ds__day)
        )
    ) subq_33
    GROUP BY
      visit__referrer_id
  ) subq_36
  ON
    subq_24.visit__referrer_id = subq_36.visit__referrer_id
  GROUP BY
    visit__referrer_id
) subq_37
