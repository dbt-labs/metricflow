-- Compute Metrics via Expressions
SELECT
  metric_time__martian_day
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_25.metric_time__martian_day, subq_38.metric_time__martian_day) AS metric_time__martian_day
    , MAX(subq_25.visits) AS visits
    , MAX(subq_38.buys) AS buys
  FROM (
    -- Pass Only Elements: ['visits', 'metric_time__day']
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['visits', 'metric_time__martian_day']
    -- Aggregate Measures
    SELECT
      subq_22.martian_day AS metric_time__martian_day
      , SUM(subq_21.visits) AS visits
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
    ) subq_21
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_22
    ON
      subq_21.metric_time__day = subq_22.ds
    GROUP BY
      subq_22.martian_day
  ) subq_25
  FULL OUTER JOIN (
    -- Pass Only Elements: ['buys', 'metric_time__day']
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['buys', 'metric_time__martian_day']
    -- Aggregate Measures
    -- Pass Only Elements: ['buys', 'metric_time__martian_day']
    SELECT
      subq_34.martian_day AS metric_time__martian_day
      , SUM(subq_32.buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_28.visits) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.ds__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_28.ds__day) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.ds__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ds__day
        , FIRST_VALUE(subq_28.metric_time__day) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.ds__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_28.user) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.ds__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.ds__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_31.mf_internal_uuid AS mf_internal_uuid
        , subq_31.buys AS buys
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
      ) subq_28
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
      ) subq_31
      ON
        (
          subq_28.user = subq_31.user
        ) AND (
          (
            subq_28.ds__day <= subq_31.ds__day
          ) AND (
            subq_28.ds__day > subq_31.ds__day - INTERVAL 7 day
          )
        )
    ) subq_32
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_34
    ON
      subq_32.metric_time__day = subq_34.ds
    GROUP BY
      subq_34.martian_day
  ) subq_38
  ON
    subq_25.metric_time__martian_day = subq_38.metric_time__martian_day
  GROUP BY
    COALESCE(subq_25.metric_time__martian_day, subq_38.metric_time__martian_day)
) subq_39
