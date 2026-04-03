test_name: test_conversion_metric_with_filter_not_in_group_by
test_filename: test_conversion_metric_rendering.py
docstring:
  Test rendering a query against a conversion metric.
sql_engine: ClickHouse
---
WITH sma_28019_cte AS (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , user_id AS user
    , referrer_id AS visit__referrer_id
    , 1 AS __visits
  FROM ***************************.fct_visits visits_source_src_28000
)

SELECT
  COALESCE(MAX(subq_37.__buys_fill_nulls_with_0), 0) AS visit_buy_conversions
FROM (
  SELECT
    SUM(__visits) AS __visits
  FROM (
    SELECT
      visits AS __visits
    FROM (
      SELECT
        visit__referrer_id
        , __visits AS visits
      FROM sma_28019_cte
    ) subq_22
    WHERE visit__referrer_id = 'ref_id_01'
  ) subq_24
) subq_25
CROSS JOIN (
  SELECT
    SUM(__buys_fill_nulls_with_0) AS __buys_fill_nulls_with_0
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
      , FIRST_VALUE(subq_29.visit__referrer_id) OVER (
        PARTITION BY
          subq_32.user
          , subq_32.metric_time__day
          , subq_32.mf_internal_uuid
        ORDER BY subq_29.metric_time__day DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS visit__referrer_id
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
      , subq_32.__buys_fill_nulls_with_0 AS __buys_fill_nulls_with_0
    FROM (
      SELECT
        metric_time__day
        , subq_27.user
        , visit__referrer_id
        , visits AS __visits
      FROM (
        SELECT
          metric_time__day
          , sma_28019_cte.user
          , visit__referrer_id
          , __visits AS visits
        FROM sma_28019_cte
      ) subq_27
      WHERE visit__referrer_id = 'ref_id_01'
    ) subq_29
    INNER JOIN (
      SELECT
        toStartOfDay(ds) AS metric_time__day
        , user_id AS user
        , 1 AS __buys_fill_nulls_with_0
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
) subq_37
