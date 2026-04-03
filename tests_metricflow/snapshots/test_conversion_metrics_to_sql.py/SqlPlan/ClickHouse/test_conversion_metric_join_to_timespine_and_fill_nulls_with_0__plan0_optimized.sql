test_name: test_conversion_metric_join_to_timespine_and_fill_nulls_with_0
test_filename: test_conversion_metrics_to_sql.py
docstring:
  Test conversion metric that joins to time spine and fills nulls with 0.
sql_engine: ClickHouse
---
WITH sma_28019_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , user_id AS user
    , 1 AS __visits_fill_nulls_with_0_join_to_timespine
  FROM ***************************.fct_visits visits_source_src_28000
)

, rss_28018_cte AS (
  SELECT
    ds AS ds__day
  FROM ***************************.mf_time_spine time_spine_src_28006
)

SELECT
  metric_time__day AS metric_time__day
  , CAST(__buys_fill_nulls_with_0_join_to_timespine AS Nullable(Float64)) / CAST(NULLIF(__visits_fill_nulls_with_0_join_to_timespine, 0) AS Nullable(Float64)) AS visit_buy_conversion_rate_7days_fill_nulls_with_0
FROM (
  SELECT
    COALESCE(subq_37.metric_time__day, subq_53.metric_time__day) AS metric_time__day
    , COALESCE(MAX(subq_37.__visits_fill_nulls_with_0_join_to_timespine), 0) AS __visits_fill_nulls_with_0_join_to_timespine
    , COALESCE(MAX(subq_53.__buys_fill_nulls_with_0_join_to_timespine), 0) AS __buys_fill_nulls_with_0_join_to_timespine
  FROM (
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_32.__visits_fill_nulls_with_0_join_to_timespine AS __visits_fill_nulls_with_0_join_to_timespine
    FROM rss_28018_cte
    LEFT OUTER JOIN (
      SELECT
        metric_time__day
        , SUM(__visits_fill_nulls_with_0_join_to_timespine) AS __visits_fill_nulls_with_0_join_to_timespine
      FROM sma_28019_cte
      GROUP BY
        metric_time__day
    ) subq_32
    ON
      rss_28018_cte.ds__day = subq_32.metric_time__day
  ) subq_37
  FULL OUTER JOIN (
    SELECT
      rss_28018_cte.ds__day AS metric_time__day
      , subq_48.__buys_fill_nulls_with_0_join_to_timespine AS __buys_fill_nulls_with_0_join_to_timespine
    FROM rss_28018_cte
    LEFT OUTER JOIN (
      SELECT
        metric_time__day
        , SUM(__buys_fill_nulls_with_0_join_to_timespine) AS __buys_fill_nulls_with_0_join_to_timespine
      FROM (
        SELECT DISTINCT
          FIRST_VALUE(sma_28019_cte.__visits_fill_nulls_with_0_join_to_timespine) OVER (
            PARTITION BY
              subq_43.user
              , subq_43.metric_time__day
              , subq_43.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS __visits_fill_nulls_with_0_join_to_timespine
          , FIRST_VALUE(sma_28019_cte.metric_time__day) OVER (
            PARTITION BY
              subq_43.user
              , subq_43.metric_time__day
              , subq_43.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS metric_time__day
          , FIRST_VALUE(sma_28019_cte.user) OVER (
            PARTITION BY
              subq_43.user
              , subq_43.metric_time__day
              , subq_43.mf_internal_uuid
            ORDER BY sma_28019_cte.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , subq_43.mf_internal_uuid AS mf_internal_uuid
          , subq_43.__buys_fill_nulls_with_0_join_to_timespine AS __buys_fill_nulls_with_0_join_to_timespine
        FROM sma_28019_cte
        INNER JOIN (
          SELECT
            toStartOfDay(ds) AS metric_time__day
            , user_id AS user
            , 1 AS __buys_fill_nulls_with_0_join_to_timespine
            , generateUUIDv4() AS mf_internal_uuid
          FROM ***************************.fct_buys buys_source_src_28000
        ) subq_43
        ON
          (
            sma_28019_cte.user = subq_43.user
          ) AND (
            (
              sma_28019_cte.metric_time__day <= subq_43.metric_time__day
            ) AND (
              sma_28019_cte.metric_time__day > addDays(subq_43.metric_time__day, -7)
            )
          )
      ) subq_44
      GROUP BY
        metric_time__day
    ) subq_48
    ON
      rss_28018_cte.ds__day = subq_48.metric_time__day
  ) subq_53
  ON
    subq_37.metric_time__day = subq_53.metric_time__day
  GROUP BY
    COALESCE(subq_37.metric_time__day, subq_53.metric_time__day)
) subq_54
