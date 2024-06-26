-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , visit__referrer_id
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_23.metric_time__day, subq_34.metric_time__day) AS metric_time__day
    , COALESCE(subq_23.visit__referrer_id, subq_34.visit__referrer_id) AS visit__referrer_id
    , MAX(subq_23.visits) AS visits
    , MAX(subq_34.buys) AS buys
  FROM (
    -- Constrain Output with WHERE
    -- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
    -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , visit__referrer_id
      , SUM(visits) AS visits
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , referrer_id AS visit__referrer_id
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
    ) subq_19
    WHERE (
      metric_time__day BETWEEN timestamp '2020-01-01' AND timestamp '2020-01-02'
    ) AND (
      visit__referrer_id = 'ref_id_01'
    )
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) subq_23
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
        FIRST_VALUE(subq_27.visits) OVER (
          PARTITION BY
            subq_30.user
            , subq_30.ds__day
            , subq_30.mf_internal_uuid
          ORDER BY subq_27.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_27.visit__referrer_id) OVER (
          PARTITION BY
            subq_30.user
            , subq_30.ds__day
            , subq_30.mf_internal_uuid
          ORDER BY subq_27.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visit__referrer_id
        , FIRST_VALUE(subq_27.ds__day) OVER (
          PARTITION BY
            subq_30.user
            , subq_30.ds__day
            , subq_30.mf_internal_uuid
          ORDER BY subq_27.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__day
        , FIRST_VALUE(subq_27.metric_time__day) OVER (
          PARTITION BY
            subq_30.user
            , subq_30.ds__day
            , subq_30.mf_internal_uuid
          ORDER BY subq_27.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_27.user) OVER (
          PARTITION BY
            subq_30.user
            , subq_30.ds__day
            , subq_30.mf_internal_uuid
          ORDER BY subq_27.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_30.mf_internal_uuid AS mf_internal_uuid
        , subq_30.buys AS buys
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
        WHERE DATE_TRUNC('day', ds) BETWEEN timestamp '2020-01-01' AND timestamp '2020-01-02'
      ) subq_27
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS ds__day
          , user_id AS user
          , 1 AS buys
          , uuid() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_30
      ON
        (
          subq_27.user = subq_30.user
        ) AND (
          (
            subq_27.ds__day <= subq_30.ds__day
          ) AND (
            subq_27.ds__day > DATE_ADD('day', -7, subq_30.ds__day)
          )
        )
    ) subq_31
    GROUP BY
      metric_time__day
      , visit__referrer_id
  ) subq_34
  ON
    (
      subq_23.visit__referrer_id = subq_34.visit__referrer_id
    ) AND (
      subq_23.metric_time__day = subq_34.metric_time__day
    )
  GROUP BY
    COALESCE(subq_23.metric_time__day, subq_34.metric_time__day)
    , COALESCE(subq_23.visit__referrer_id, subq_34.visit__referrer_id)
) subq_35
