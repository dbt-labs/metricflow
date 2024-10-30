-- Compute Metrics via Expressions
SELECT
  metric_time__martian_day
  , CAST(buys AS DOUBLE) / CAST(NULLIF(visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_24.metric_time__martian_day, subq_36.metric_time__martian_day) AS metric_time__martian_day
    , MAX(subq_24.visits) AS visits
    , MAX(subq_36.buys) AS buys
  FROM (
    -- Pass Only Elements: ['visits', 'metric_time__day']
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['visits', 'metric_time__martian_day']
    -- Aggregate Measures
    SELECT
      subq_21.martian_day AS metric_time__martian_day
      , SUM(subq_20.visits) AS visits
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS visits
      FROM ***************************.fct_visits visits_source_src_28000
    ) subq_20
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_21
    ON
      subq_20.metric_time__day = subq_21.ds
    GROUP BY
      subq_21.martian_day
  ) subq_24
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['buys', 'metric_time__martian_day']
    -- Pass Only Elements: ['buys', 'metric_time__martian_day']
    -- Aggregate Measures
    SELECT
      metric_time__martian_day
      , SUM(buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_28.visits) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.metric_time__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_28.metric_time__martian_day) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.metric_time__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__martian_day
        , FIRST_VALUE(subq_28.metric_time__day) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.metric_time__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_28.user) OVER (
          PARTITION BY
            subq_31.user
            , subq_31.metric_time__day
            , subq_31.mf_internal_uuid
          ORDER BY subq_28.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_31.mf_internal_uuid AS mf_internal_uuid
        , subq_31.buys AS buys
      FROM (
        -- Metric Time Dimension 'ds'
        -- Join to Custom Granularity Dataset
        -- Pass Only Elements: ['visits', 'metric_time__day', 'metric_time__martian_day', 'user']
        SELECT
          subq_26.martian_day AS metric_time__martian_day
          , subq_25.ds__day AS metric_time__day
          , subq_25.user AS user
          , subq_25.visits AS visits
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          SELECT
            1 AS visits
            , DATE_TRUNC('day', ds) AS ds__day
            , user_id AS user
          FROM ***************************.fct_visits visits_source_src_28000
        ) subq_25
        LEFT OUTER JOIN
          ***************************.mf_time_spine subq_26
        ON
          subq_25.ds__day = subq_26.ds
      ) subq_28
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS buys
          , uuid() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_31
      ON
        (
          subq_28.user = subq_31.user
        ) AND (
          (
            subq_28.metric_time__day <= subq_31.metric_time__day
          ) AND (
            subq_28.metric_time__day > DATE_ADD('day', -7, subq_31.metric_time__day)
          )
        )
    ) subq_32
    GROUP BY
      metric_time__martian_day
  ) subq_36
  ON
    subq_24.metric_time__martian_day = subq_36.metric_time__martian_day
  GROUP BY
    COALESCE(subq_24.metric_time__martian_day, subq_36.metric_time__martian_day)
) subq_37
