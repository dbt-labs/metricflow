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
    -- Pass Only Elements: ['buys', 'metric_time__day']
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['buys', 'metric_time__martian_day']
    -- Aggregate Measures
    SELECT
      subq_33.martian_day AS metric_time__martian_day
      , SUM(subq_31.buys) AS buys
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
        -- Pass Only Elements: ['visits', 'ds__day', 'metric_time__day', 'user']
        SELECT
          DATE_TRUNC('day', ds) AS ds__day
          , DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) subq_27
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
      ) subq_30
      ON
        (
          subq_27.user = subq_30.user
        ) AND (
          (
            subq_27.ds__day <= subq_30.ds__day
          ) AND (
            subq_27.ds__day > subq_30.ds__day - INTERVAL 7 day
          )
        )
    ) subq_31
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_33
    ON
      subq_31.metric_time__day = subq_33.ds
    GROUP BY
      subq_33.martian_day
  ) subq_36
  ON
    subq_24.metric_time__martian_day = subq_36.metric_time__martian_day
  GROUP BY
    COALESCE(subq_24.metric_time__martian_day, subq_36.metric_time__martian_day)
) subq_37
