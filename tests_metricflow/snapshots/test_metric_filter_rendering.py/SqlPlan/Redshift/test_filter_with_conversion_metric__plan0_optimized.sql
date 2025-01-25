test_name: test_filter_with_conversion_metric
test_filename: test_metric_filter_rendering.py
sql_engine: Redshift
---
-- Constrain Output with WHERE
-- Pass Only Elements: ['listings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(listings) AS listings
FROM (
  -- Join Standard Outputs
  SELECT
    CAST(nr_subq_34.buys AS DOUBLE PRECISION) / CAST(NULLIF(nr_subq_34.visits, 0) AS DOUBLE PRECISION) AS user__visit_buy_conversion_rate
    , nr_subq_31.listings AS listings
  FROM (
    -- Read Elements From Semantic Model 'listings_latest'
    -- Metric Time Dimension 'ds'
    SELECT
      user_id AS user
      , 1 AS listings
    FROM ***************************.dim_listings_latest listings_latest_src_28000
  ) nr_subq_31
  LEFT OUTER JOIN (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(nr_subq_32.user, nr_subq_33.user) AS user
      , MAX(nr_subq_32.visits) AS visits
      , MAX(nr_subq_33.buys) AS buys
    FROM (
      -- Aggregate Measures
      SELECT
        nr_subq_14.user
        , SUM(visits) AS visits
      FROM (
        -- Read Elements From Semantic Model 'visits_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements: ['visits', 'user']
        SELECT
          user_id AS user
          , 1 AS visits
        FROM ***************************.fct_visits visits_source_src_28000
      ) nr_subq_14
      GROUP BY
        nr_subq_14.user
    ) nr_subq_32
    FULL OUTER JOIN (
      -- Find conversions for user within the range of INF
      -- Pass Only Elements: ['buys', 'user']
      -- Aggregate Measures
      SELECT
        nr_subq_20.user
        , SUM(buys) AS buys
      FROM (
        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
        SELECT DISTINCT
          FIRST_VALUE(nr_subq_17.visits) OVER (
            PARTITION BY
              nr_subq_19.user
              , nr_subq_19.metric_time__day
              , nr_subq_19.mf_internal_uuid
            ORDER BY nr_subq_17.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS visits
          , FIRST_VALUE(nr_subq_17.metric_time__day) OVER (
            PARTITION BY
              nr_subq_19.user
              , nr_subq_19.metric_time__day
              , nr_subq_19.mf_internal_uuid
            ORDER BY nr_subq_17.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS metric_time__day
          , FIRST_VALUE(nr_subq_17.user) OVER (
            PARTITION BY
              nr_subq_19.user
              , nr_subq_19.metric_time__day
              , nr_subq_19.mf_internal_uuid
            ORDER BY nr_subq_17.metric_time__day DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS user
          , nr_subq_19.mf_internal_uuid AS mf_internal_uuid
          , nr_subq_19.buys AS buys
        FROM (
          -- Read Elements From Semantic Model 'visits_source'
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['visits', 'metric_time__day', 'user']
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , user_id AS user
            , 1 AS visits
          FROM ***************************.fct_visits visits_source_src_28000
        ) nr_subq_17
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
        ) nr_subq_19
        ON
          (
            nr_subq_17.user = nr_subq_19.user
          ) AND (
            (nr_subq_17.metric_time__day <= nr_subq_19.metric_time__day)
          )
      ) nr_subq_20
      GROUP BY
        nr_subq_20.user
    ) nr_subq_33
    ON
      nr_subq_32.user = nr_subq_33.user
    GROUP BY
      COALESCE(nr_subq_32.user, nr_subq_33.user)
  ) nr_subq_34
  ON
    nr_subq_31.user = nr_subq_34.user
) nr_subq_37
WHERE user__visit_buy_conversion_rate > 2
