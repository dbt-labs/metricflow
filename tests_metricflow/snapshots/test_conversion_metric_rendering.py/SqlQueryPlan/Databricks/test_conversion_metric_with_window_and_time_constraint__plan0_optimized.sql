-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , visit__referrer_id
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_31.metric_time__day, subq_42.metric_time__day) AS metric_time__day
    , COALESCE(subq_31.visit__referrer_id, subq_42.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_31.visits) AS visits
    , MAX(subq_42.buys) AS buys
  FROM (
    -- Constrain Output with WHERE
    -- Aggregate Measures
    SELECT
      metric_time__day
      , visit__referrer_id
      , SUM(visits) AS visits
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
      -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , referrer_id AS visit__referrer_id
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
      WHERE DATE_TRUNC('day', ds) BETWEEN '2020-01-01' AND '2020-01-02'
    ) subq_29
    WHERE visit__referrer_id = 'ref_id_01'
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) subq_31
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['buys', 'visit__referrer_id', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , visit__referrer_id
      , SUM(buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_35.visits) OVER (
          PARTITION BY
            subq_38.user
            , subq_38.ds__day
            , subq_38.mf_internal_uuid
          ORDER BY subq_35.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_35.visit__referrer_id) OVER (
          PARTITION BY
            subq_38.user
            , subq_38.ds__day
            , subq_38.mf_internal_uuid
          ORDER BY subq_35.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(subq_35.ds__day) OVER (
          PARTITION BY
            subq_38.user
            , subq_38.ds__day
            , subq_38.mf_internal_uuid
          ORDER BY subq_35.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__day
        , FIRST_VALUE(subq_35.metric_time__day) OVER (
          PARTITION BY
            subq_38.user
            , subq_38.ds__day
            , subq_38.mf_internal_uuid
          ORDER BY subq_35.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_35.user) OVER (
          PARTITION BY
            subq_38.user
            , subq_38.ds__day
            , subq_38.mf_internal_uuid
          ORDER BY subq_35.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_38.mf_internal_uuid AS mf_internal_uuid
        , subq_38.buys AS buys
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
        -- Pass Only Elements: ['visits', 'visit__referrer_id', 'ds__day', 'metric_time__day', 'user']
        SELECT
          DATE_TRUNC('day', ds) AS ds__day
          , DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , referrer_id AS visit__referrer_id
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
        WHERE DATE_TRUNC('day', ds) BETWEEN '2020-01-01' AND '2020-01-02'
      ) subq_35
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
      ) subq_38
      ON
        (
          subq_35.user = subq_38.user
        ) AND (
          (
            subq_35.ds__day <= subq_38.ds__day
          ) AND (
            subq_35.ds__day > DATEADD(day, -7, subq_38.ds__day)
          )
        )
    ) subq_39
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) subq_42
  ON
    (
      subq_31.visit__referrer_id = subq_42.visit__referrer_id
    ) AND (
      subq_31.metric_time__day = subq_42.metric_time__day
    )
  GROUP BY
    COALESCE(subq_31.metric_time__day, subq_42.metric_time__day)
    , COALESCE(subq_31.visit__referrer_id, subq_42.visit__referrer_id)
) subq_43
