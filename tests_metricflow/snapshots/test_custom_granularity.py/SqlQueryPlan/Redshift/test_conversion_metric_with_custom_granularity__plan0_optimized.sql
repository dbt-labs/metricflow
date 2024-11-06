-- Compute Metrics via Expressions
SELECT
  metric_time__martian_day
  , CAST(buys AS DOUBLE PRECISION) / CAST(NULLIF(visits, 0) AS DOUBLE PRECISION) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_21.metric_time__martian_day, subq_32.metric_time__martian_day) AS metric_time__martian_day
    , MAX(subq_21.visits) AS visits
    , MAX(subq_32.buys) AS buys
  FROM (
    -- Metric Time Dimension 'ds'
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['visits', 'metric_time__martian_day']
    -- Aggregate Measures
    SELECT
      subq_18.martian_day AS metric_time__martian_day
      , SUM(subq_17.visits) AS visits
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      SELECT
        1 AS visits
        , DATE_TRUNC('day', ds) AS ds__day
      FROM ***************************.fct_visits visits_source_src_28000
    ) subq_17
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_18
    ON
      subq_17.ds__day = subq_18.ds
    GROUP BY
      subq_18.martian_day
  ) subq_21
  FULL OUTER JOIN (
    -- Find conversions for user within the range of 7 day
    -- Pass Only Elements: ['buys', 'metric_time__martian_day']
    -- Aggregate Measures
    SELECT
      metric_time__martian_day
      , SUM(buys) AS buys
    FROM (
      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
      SELECT DISTINCT
        FIRST_VALUE(subq_25.visits) OVER (
          PARTITION BY
            subq_28.user
            , subq_28.metric_time__day
            , subq_28.mf_internal_uuid
          ORDER BY subq_25.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS visits
        , FIRST_VALUE(subq_25.metric_time__martian_day) OVER (
          PARTITION BY
            subq_28.user
            , subq_28.metric_time__day
            , subq_28.mf_internal_uuid
          ORDER BY subq_25.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__martian_day
        , FIRST_VALUE(subq_25.metric_time__day) OVER (
          PARTITION BY
            subq_28.user
            , subq_28.metric_time__day
            , subq_28.mf_internal_uuid
          ORDER BY subq_25.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__day
        , FIRST_VALUE(subq_25.user) OVER (
          PARTITION BY
            subq_28.user
            , subq_28.metric_time__day
            , subq_28.mf_internal_uuid
          ORDER BY subq_25.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS user
        , subq_28.mf_internal_uuid AS mf_internal_uuid
        , subq_28.buys AS buys
      FROM (
        -- Metric Time Dimension 'ds'
        -- Join to Custom Granularity Dataset
        -- Pass Only Elements: ['visits', 'metric_time__day', 'metric_time__martian_day', 'user']
        SELECT
          subq_23.martian_day AS metric_time__martian_day
          , subq_22.ds__day AS metric_time__day
          , subq_22.user AS user
          , subq_22.visits AS visits
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          SELECT
            1 AS visits
            , DATE_TRUNC('day', ds) AS ds__day
            , user_id AS user
          FROM ***************************.fct_visits visits_source_src_28000
        ) subq_22
        LEFT OUTER JOIN
          ***************************.mf_time_spine subq_23
        ON
          subq_22.ds__day = subq_23.ds
      ) subq_25
      INNER JOIN (
        -- Read Elements From Semantic Model 'buys_source'
        -- Metric Time Dimension 'ds'
        -- Add column with generated UUID
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , user_id AS user
          , 1 AS buys
          , CONCAT(CAST(RANDOM()*100000000 AS INT)::VARCHAR,CAST(RANDOM()*100000000 AS INT)::VARCHAR) AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_28
      ON
        (
          subq_25.user = subq_28.user
        ) AND (
          (
            subq_25.metric_time__day <= subq_28.metric_time__day
          ) AND (
            subq_25.metric_time__day > DATEADD(day, -7, subq_28.metric_time__day)
          )
        )
    ) subq_29
    GROUP BY
      metric_time__martian_day
  ) subq_32
  ON
    subq_21.metric_time__martian_day = subq_32.metric_time__martian_day
  GROUP BY
    COALESCE(subq_21.metric_time__martian_day, subq_32.metric_time__martian_day)
) subq_33
