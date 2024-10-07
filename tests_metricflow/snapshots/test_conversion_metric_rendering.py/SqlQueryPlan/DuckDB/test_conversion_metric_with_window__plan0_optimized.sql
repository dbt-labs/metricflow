-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_16.metric_time__day, subq_24.metric_time__day) AS metric_time__day
    , MAX(subq_16.visits) AS visits
    , MAX(subq_24.buys) AS buys
  FROM (
    -- Constrain Output with WHERE
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(visits) AS visits
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['visits', 'metric_time__day']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
    ) subq_14
    WHERE metric_time__day = '2020-01-01'
    GROUP BY
      metric_time__day
  ) subq_16
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['buys', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_18.visits) OVER (
          PARTITION BY
            subq_21.user
            , subq_21.ds__day
            , subq_21.mf_internal_uuid
          ORDER BY subq_18.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_18.ds__day) OVER (
          PARTITION BY
            subq_21.user
            , subq_21.ds__day
            , subq_21.mf_internal_uuid
          ORDER BY subq_18.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__day
        , FIRST_VALUE(subq_18.metric_time__day) OVER (
          PARTITION BY
            subq_21.user
            , subq_21.ds__day
            , subq_21.mf_internal_uuid
          ORDER BY subq_18.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_18.user) OVER (
          PARTITION BY
            subq_21.user
            , subq_21.ds__day
            , subq_21.mf_internal_uuid
          ORDER BY subq_18.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_21.mf_internal_uuid AS mf_internal_uuid
        , subq_21.buys AS buys
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['visits', 'ds__day', 'metric_time__day', 'user']
        SELECT
          DATE_TRUNC('day', ds) AS ds__day
          , DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) subq_18
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
      ) subq_21
      ON
        (
          subq_18.user = subq_21.user
        ) AND (
          (
            subq_18.ds__day <= subq_21.ds__day
          ) AND (
            subq_18.ds__day > subq_21.ds__day - INTERVAL 7 day
          )
        )
    ) subq_22
    GROUP BY
      metric_time__day
  ) subq_24
  ON
    subq_16.metric_time__day = subq_24.metric_time__day
  GROUP BY
    COALESCE(subq_16.metric_time__day, subq_24.metric_time__day)
) subq_25
