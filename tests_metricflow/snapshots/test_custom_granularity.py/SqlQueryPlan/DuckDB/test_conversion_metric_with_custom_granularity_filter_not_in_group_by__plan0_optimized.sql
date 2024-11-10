test_name: test_conversion_metric_with_custom_granularity_filter_not_in_group_by
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Combine Aggregated Outputs
-- Compute Metrics via Expressions
SELECT
  CAST(MAX(subq_36.buys) AS DOUBLE) / CAST(NULLIF(MAX(subq_24.visits), 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['visits',]
  -- Aggregate Measures
  SELECT
    SUM(visits) AS visits
  FROM (
    -- Metric Time Dimension 'ds'
    -- Join to Custom Granularity Dataset
    SELECT
      subq_19.visits AS visits
      , subq_20.martian_day AS metric_time__martian_day
    FROM (
      -- Read Elements From Semantic Model 'visits_source'
      SELECT
        1 AS visits
        , DATE_TRUNC('day', ds) AS ds__day
      FROM ***************************.fct_visits visits_source_src_28000
    ) subq_19
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_20
    ON
      subq_19.ds__day = subq_20.ds
  ) subq_21
  WHERE metric_time__martian_day = '2020-01-01'
) subq_24
CROSS JOIN (
  -- Find conversions for user within the range of 7 day
  -- Pass Only Elements: ['buys',]
  -- Aggregate Measures
  SELECT
    SUM(buys) AS buys
  FROM (
    -- Dedupe the fanout with mf_internal_uuid in the conversion data set
    SELECT DISTINCT
      FIRST_VALUE(subq_29.visits) OVER (
        PARTITION BY
          subq_32.user
          , subq_32.metric_time__day
          , subq_32.mf_internal_uuid
        ORDER BY subq_29.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visits
      , FIRST_VALUE(subq_29.metric_time__martian_day) OVER (
        PARTITION BY
          subq_32.user
          , subq_32.metric_time__day
          , subq_32.mf_internal_uuid
        ORDER BY subq_29.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__martian_day
      , FIRST_VALUE(subq_29.metric_time__day) OVER (
        PARTITION BY
          subq_32.user
          , subq_32.metric_time__day
          , subq_32.mf_internal_uuid
        ORDER BY subq_29.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS metric_time__day
      , FIRST_VALUE(subq_29.user) OVER (
        PARTITION BY
          subq_32.user
          , subq_32.metric_time__day
          , subq_32.mf_internal_uuid
        ORDER BY subq_29.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS user
      , subq_32.mf_internal_uuid AS mf_internal_uuid
      , subq_32.buys AS buys
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['visits', 'metric_time__day', 'metric_time__martian_day', 'user']
      SELECT
        metric_time__martian_day
        , metric_time__day
        , subq_27.user
        , visits
      FROM (
        -- Metric Time Dimension 'ds'
        -- Join to Custom Granularity Dataset
        SELECT
          subq_25.ds__day AS metric_time__day
          , subq_25.user AS user
          , subq_25.visits AS visits
          , subq_26.martian_day AS metric_time__martian_day
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
      ) subq_27
      WHERE metric_time__martian_day = '2020-01-01'
    ) subq_29
    INNER JOIN (
      -- Read Elements From Semantic Model 'buys_source'
      -- Metric Time Dimension 'ds'
      -- Add column with generated UUID
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , user_id AS user
        , 1 AS buys
        , GEN_RANDOM_UUID() AS mf_internal_uuid
      FROM ***************************.fct_buys buys_source_src_28000
    ) subq_32
    ON
      (
        subq_29.user = subq_32.user
      ) AND (
        (
          subq_29.metric_time__day <= subq_32.metric_time__day
        ) AND (
          subq_29.metric_time__day > subq_32.metric_time__day - INTERVAL 7 day
        )
      )
  ) subq_33
) subq_36
