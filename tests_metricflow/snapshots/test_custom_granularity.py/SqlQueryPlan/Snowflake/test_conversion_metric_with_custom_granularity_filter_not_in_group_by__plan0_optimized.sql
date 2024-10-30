-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  CAST(MAX(subq_38.buys) AS DOUBLE) / CAST(NULLIF(MAX(subq_26.visits), 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['visits',]
  -- Aggregate Measures
  SELECT
    SUM(visits) AS visits
  FROM (
    -- Pass Only Elements: ['visits', 'metric_time__day']
    -- Join to Custom Granularity Dataset
    SELECT
      subq_21.visits AS visits
      , subq_22.martian_day AS metric_time__martian_day
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
  ) subq_23
  WHERE metric_time__martian_day = '2020-01-01'
) subq_26
CROSS JOIN (
  -- Find conversions for user within the range of 7 day
  -- Pass Only Elements: ['buys',]
  -- Aggregate Measures
  SELECT
    SUM(buys) AS buys
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      FIRST_VALUE(subq_31.visits) OVER (
        PARTITION BY
          subq_34.user
          , subq_34.ds__day
          , subq_34.mf_internal_uuid
        ORDER BY subq_31.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(subq_31.metric_time__martian_day) OVER (
        PARTITION BY
          subq_34.user
          , subq_34.ds__day
          , subq_34.mf_internal_uuid
        ORDER BY subq_31.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__martian_day
      , FIRST_VALUE(subq_31.ds__day) OVER (
        PARTITION BY
          subq_34.user
          , subq_34.ds__day
          , subq_34.mf_internal_uuid
        ORDER BY subq_31.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ds__day
      , FIRST_VALUE(subq_31.metric_time__day) OVER (
        PARTITION BY
          subq_34.user
          , subq_34.ds__day
          , subq_34.mf_internal_uuid
        ORDER BY subq_31.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__day
      , FIRST_VALUE(subq_31.user) OVER (
        PARTITION BY
          subq_34.user
          , subq_34.ds__day
          , subq_34.mf_internal_uuid
        ORDER BY subq_31.ds__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , subq_34.mf_internal_uuid AS mf_internal_uuid
      , subq_34.buys AS buys
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['visits', 'ds__day', 'metric_time__day', 'metric_time__martian_day', 'user']
      SELECT
        metric_time__martian_day
        , ds__day
        , metric_time__day
        , subq_29.user
        , visits
      FROM (
        -- Metric Time Dimension 'ds'
        -- Join to Custom Granularity Dataset
        SELECT
          subq_27.ds__day AS ds__day
          , subq_27.ds__day AS metric_time__day
          , subq_27.user AS user
          , subq_27.visits AS visits
          , subq_28.martian_day AS metric_time__martian_day
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          SELECT
            1 AS visits
            , DATE_TRUNC('day', ds) AS ds__day
            , user_id AS user
          FROM ***************************.fct_visits visits_source_src_28000
        ) subq_27
        LEFT OUTER JOIN
          ***************************.mf_time_spine subq_28
        ON
          subq_27.ds__day = subq_28.ds
      ) subq_29
      WHERE metric_time__martian_day = '2020-01-01'
    ) subq_31
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
    ) subq_34
    ON
      (
        subq_31.user = subq_34.user
      ) AND (
        (
          subq_31.ds__day <= subq_34.ds__day
        ) AND (
          subq_31.ds__day > DATEADD(day, -7, subq_34.ds__day)
        )
      )
  ) subq_35
) subq_38
