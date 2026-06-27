test_name: test_conversion_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: ClickHouse
---
WITH sma_28019_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , user_id AS user
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  metric_time__alien_day AS metric_time__alien_day
  , CAST(__buys AS Nullable(Float64)) / CAST(NULLIF(__visits, 0) AS Nullable(Float64)) AS visit_buy_conversion_rate_7days
FROM (
  SELECT
    COALESCE(subq_25.metric_time__alien_day, subq_37.metric_time__alien_day) AS metric_time__alien_day
    , MAX(subq_25.__visits) AS __visits
    , MAX(subq_37.__buys) AS __buys
  FROM (
    SELECT
      subq_21.alien_day AS metric_time__alien_day
      , SUM(sma_28019_cte.__visits) AS __visits
    FROM sma_28019_cte
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_21
    ON
      sma_28019_cte.metric_time__day = subq_21.ds
    GROUP BY
      subq_21.alien_day
  ) subq_25
  FULL OUTER JOIN (
    SELECT
      metric_time__alien_day
      , SUM(__buys) AS __buys
    FROM (
      SELECT DISTINCT
        FIRST_VALUE(subq_29.__visits) OVER (
          PARTITION BY
            subq_32.user
            , subq_32.metric_time__day
            , subq_32.mf_internal_uuid
          ORDER BY subq_29.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS __visits
        , FIRST_VALUE(subq_29.metric_time__alien_day) OVER (
          PARTITION BY
            subq_32.user
            , subq_32.metric_time__day
            , subq_32.mf_internal_uuid
          ORDER BY subq_29.metric_time__day DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS metric_time__alien_day
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
        , subq_32.__buys AS __buys
      FROM (
        SELECT
          subq_26.alien_day AS metric_time__alien_day
          , sma_28019_cte.metric_time__day AS metric_time__day
          , sma_28019_cte.user AS user
          , sma_28019_cte.__visits AS __visits
        FROM sma_28019_cte
        LEFT OUTER JOIN
          ***************************.mf_time_spine subq_26
        ON
          sma_28019_cte.metric_time__day = subq_26.ds
      ) subq_29
      INNER JOIN (
        SELECT
          toStartOfDay(ds) AS metric_time__day
          , user_id AS user
          , 1 AS __buys
          , generateUUIDv4() AS mf_internal_uuid
        FROM ***************************.fct_buys buys_source_src_28000
      ) subq_32
      ON
        (
          subq_29.user = subq_32.user
        ) AND (
          (
            subq_29.metric_time__day <= subq_32.metric_time__day
          ) AND (
            subq_29.metric_time__day > addDays(subq_32.metric_time__day, -7)
          )
        )
    ) subq_33
    GROUP BY
      metric_time__alien_day
  ) subq_37
  ON
    subq_25.metric_time__alien_day = subq_37.metric_time__alien_day
  GROUP BY
    COALESCE(subq_25.metric_time__alien_day, subq_37.metric_time__alien_day)
) subq_38
