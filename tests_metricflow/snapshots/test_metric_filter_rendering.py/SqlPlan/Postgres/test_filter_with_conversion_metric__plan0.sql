test_name: test_filter_with_conversion_metric
test_filename: test_metric_filter_rendering.py
sql_engine: Postgres
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_30.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(nr_subq_29.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      nr_subq_28.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        nr_subq_27.ds__day
        , nr_subq_27.ds__week
        , nr_subq_27.ds__month
        , nr_subq_27.ds__quarter
        , nr_subq_27.ds__year
        , nr_subq_27.ds__extract_year
        , nr_subq_27.ds__extract_quarter
        , nr_subq_27.ds__extract_month
        , nr_subq_27.ds__extract_day
        , nr_subq_27.ds__extract_dow
        , nr_subq_27.ds__extract_doy
        , nr_subq_27.created_at__day
        , nr_subq_27.created_at__week
        , nr_subq_27.created_at__month
        , nr_subq_27.created_at__quarter
        , nr_subq_27.created_at__year
        , nr_subq_27.created_at__extract_year
        , nr_subq_27.created_at__extract_quarter
        , nr_subq_27.created_at__extract_month
        , nr_subq_27.created_at__extract_day
        , nr_subq_27.created_at__extract_dow
        , nr_subq_27.created_at__extract_doy
        , nr_subq_27.listing__ds__day
        , nr_subq_27.listing__ds__week
        , nr_subq_27.listing__ds__month
        , nr_subq_27.listing__ds__quarter
        , nr_subq_27.listing__ds__year
        , nr_subq_27.listing__ds__extract_year
        , nr_subq_27.listing__ds__extract_quarter
        , nr_subq_27.listing__ds__extract_month
        , nr_subq_27.listing__ds__extract_day
        , nr_subq_27.listing__ds__extract_dow
        , nr_subq_27.listing__ds__extract_doy
        , nr_subq_27.listing__created_at__day
        , nr_subq_27.listing__created_at__week
        , nr_subq_27.listing__created_at__month
        , nr_subq_27.listing__created_at__quarter
        , nr_subq_27.listing__created_at__year
        , nr_subq_27.listing__created_at__extract_year
        , nr_subq_27.listing__created_at__extract_quarter
        , nr_subq_27.listing__created_at__extract_month
        , nr_subq_27.listing__created_at__extract_day
        , nr_subq_27.listing__created_at__extract_dow
        , nr_subq_27.listing__created_at__extract_doy
        , nr_subq_27.metric_time__day
        , nr_subq_27.metric_time__week
        , nr_subq_27.metric_time__month
        , nr_subq_27.metric_time__quarter
        , nr_subq_27.metric_time__year
        , nr_subq_27.metric_time__extract_year
        , nr_subq_27.metric_time__extract_quarter
        , nr_subq_27.metric_time__extract_month
        , nr_subq_27.metric_time__extract_day
        , nr_subq_27.metric_time__extract_dow
        , nr_subq_27.metric_time__extract_doy
        , nr_subq_27.listing
        , nr_subq_27.user
        , nr_subq_27.listing__user
        , nr_subq_27.country_latest
        , nr_subq_27.is_lux_latest
        , nr_subq_27.capacity_latest
        , nr_subq_27.listing__country_latest
        , nr_subq_27.listing__is_lux_latest
        , nr_subq_27.listing__capacity_latest
        , nr_subq_27.user__visit_buy_conversion_rate
        , nr_subq_27.listings
        , nr_subq_27.largest_listing
        , nr_subq_27.smallest_listing
      FROM (
        -- Join Standard Outputs
        SELECT
          nr_subq_26.user__visit_buy_conversion_rate AS user__visit_buy_conversion_rate
          , nr_subq_12.ds__day AS ds__day
          , nr_subq_12.ds__week AS ds__week
          , nr_subq_12.ds__month AS ds__month
          , nr_subq_12.ds__quarter AS ds__quarter
          , nr_subq_12.ds__year AS ds__year
          , nr_subq_12.ds__extract_year AS ds__extract_year
          , nr_subq_12.ds__extract_quarter AS ds__extract_quarter
          , nr_subq_12.ds__extract_month AS ds__extract_month
          , nr_subq_12.ds__extract_day AS ds__extract_day
          , nr_subq_12.ds__extract_dow AS ds__extract_dow
          , nr_subq_12.ds__extract_doy AS ds__extract_doy
          , nr_subq_12.created_at__day AS created_at__day
          , nr_subq_12.created_at__week AS created_at__week
          , nr_subq_12.created_at__month AS created_at__month
          , nr_subq_12.created_at__quarter AS created_at__quarter
          , nr_subq_12.created_at__year AS created_at__year
          , nr_subq_12.created_at__extract_year AS created_at__extract_year
          , nr_subq_12.created_at__extract_quarter AS created_at__extract_quarter
          , nr_subq_12.created_at__extract_month AS created_at__extract_month
          , nr_subq_12.created_at__extract_day AS created_at__extract_day
          , nr_subq_12.created_at__extract_dow AS created_at__extract_dow
          , nr_subq_12.created_at__extract_doy AS created_at__extract_doy
          , nr_subq_12.listing__ds__day AS listing__ds__day
          , nr_subq_12.listing__ds__week AS listing__ds__week
          , nr_subq_12.listing__ds__month AS listing__ds__month
          , nr_subq_12.listing__ds__quarter AS listing__ds__quarter
          , nr_subq_12.listing__ds__year AS listing__ds__year
          , nr_subq_12.listing__ds__extract_year AS listing__ds__extract_year
          , nr_subq_12.listing__ds__extract_quarter AS listing__ds__extract_quarter
          , nr_subq_12.listing__ds__extract_month AS listing__ds__extract_month
          , nr_subq_12.listing__ds__extract_day AS listing__ds__extract_day
          , nr_subq_12.listing__ds__extract_dow AS listing__ds__extract_dow
          , nr_subq_12.listing__ds__extract_doy AS listing__ds__extract_doy
          , nr_subq_12.listing__created_at__day AS listing__created_at__day
          , nr_subq_12.listing__created_at__week AS listing__created_at__week
          , nr_subq_12.listing__created_at__month AS listing__created_at__month
          , nr_subq_12.listing__created_at__quarter AS listing__created_at__quarter
          , nr_subq_12.listing__created_at__year AS listing__created_at__year
          , nr_subq_12.listing__created_at__extract_year AS listing__created_at__extract_year
          , nr_subq_12.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
          , nr_subq_12.listing__created_at__extract_month AS listing__created_at__extract_month
          , nr_subq_12.listing__created_at__extract_day AS listing__created_at__extract_day
          , nr_subq_12.listing__created_at__extract_dow AS listing__created_at__extract_dow
          , nr_subq_12.listing__created_at__extract_doy AS listing__created_at__extract_doy
          , nr_subq_12.metric_time__day AS metric_time__day
          , nr_subq_12.metric_time__week AS metric_time__week
          , nr_subq_12.metric_time__month AS metric_time__month
          , nr_subq_12.metric_time__quarter AS metric_time__quarter
          , nr_subq_12.metric_time__year AS metric_time__year
          , nr_subq_12.metric_time__extract_year AS metric_time__extract_year
          , nr_subq_12.metric_time__extract_quarter AS metric_time__extract_quarter
          , nr_subq_12.metric_time__extract_month AS metric_time__extract_month
          , nr_subq_12.metric_time__extract_day AS metric_time__extract_day
          , nr_subq_12.metric_time__extract_dow AS metric_time__extract_dow
          , nr_subq_12.metric_time__extract_doy AS metric_time__extract_doy
          , nr_subq_12.listing AS listing
          , nr_subq_12.user AS user
          , nr_subq_12.listing__user AS listing__user
          , nr_subq_12.country_latest AS country_latest
          , nr_subq_12.is_lux_latest AS is_lux_latest
          , nr_subq_12.capacity_latest AS capacity_latest
          , nr_subq_12.listing__country_latest AS listing__country_latest
          , nr_subq_12.listing__is_lux_latest AS listing__is_lux_latest
          , nr_subq_12.listing__capacity_latest AS listing__capacity_latest
          , nr_subq_12.listings AS listings
          , nr_subq_12.largest_listing AS largest_listing
          , nr_subq_12.smallest_listing AS smallest_listing
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            nr_subq_28007.ds__day
            , nr_subq_28007.ds__week
            , nr_subq_28007.ds__month
            , nr_subq_28007.ds__quarter
            , nr_subq_28007.ds__year
            , nr_subq_28007.ds__extract_year
            , nr_subq_28007.ds__extract_quarter
            , nr_subq_28007.ds__extract_month
            , nr_subq_28007.ds__extract_day
            , nr_subq_28007.ds__extract_dow
            , nr_subq_28007.ds__extract_doy
            , nr_subq_28007.created_at__day
            , nr_subq_28007.created_at__week
            , nr_subq_28007.created_at__month
            , nr_subq_28007.created_at__quarter
            , nr_subq_28007.created_at__year
            , nr_subq_28007.created_at__extract_year
            , nr_subq_28007.created_at__extract_quarter
            , nr_subq_28007.created_at__extract_month
            , nr_subq_28007.created_at__extract_day
            , nr_subq_28007.created_at__extract_dow
            , nr_subq_28007.created_at__extract_doy
            , nr_subq_28007.listing__ds__day
            , nr_subq_28007.listing__ds__week
            , nr_subq_28007.listing__ds__month
            , nr_subq_28007.listing__ds__quarter
            , nr_subq_28007.listing__ds__year
            , nr_subq_28007.listing__ds__extract_year
            , nr_subq_28007.listing__ds__extract_quarter
            , nr_subq_28007.listing__ds__extract_month
            , nr_subq_28007.listing__ds__extract_day
            , nr_subq_28007.listing__ds__extract_dow
            , nr_subq_28007.listing__ds__extract_doy
            , nr_subq_28007.listing__created_at__day
            , nr_subq_28007.listing__created_at__week
            , nr_subq_28007.listing__created_at__month
            , nr_subq_28007.listing__created_at__quarter
            , nr_subq_28007.listing__created_at__year
            , nr_subq_28007.listing__created_at__extract_year
            , nr_subq_28007.listing__created_at__extract_quarter
            , nr_subq_28007.listing__created_at__extract_month
            , nr_subq_28007.listing__created_at__extract_day
            , nr_subq_28007.listing__created_at__extract_dow
            , nr_subq_28007.listing__created_at__extract_doy
            , nr_subq_28007.ds__day AS metric_time__day
            , nr_subq_28007.ds__week AS metric_time__week
            , nr_subq_28007.ds__month AS metric_time__month
            , nr_subq_28007.ds__quarter AS metric_time__quarter
            , nr_subq_28007.ds__year AS metric_time__year
            , nr_subq_28007.ds__extract_year AS metric_time__extract_year
            , nr_subq_28007.ds__extract_quarter AS metric_time__extract_quarter
            , nr_subq_28007.ds__extract_month AS metric_time__extract_month
            , nr_subq_28007.ds__extract_day AS metric_time__extract_day
            , nr_subq_28007.ds__extract_dow AS metric_time__extract_dow
            , nr_subq_28007.ds__extract_doy AS metric_time__extract_doy
            , nr_subq_28007.listing
            , nr_subq_28007.user
            , nr_subq_28007.listing__user
            , nr_subq_28007.country_latest
            , nr_subq_28007.is_lux_latest
            , nr_subq_28007.capacity_latest
            , nr_subq_28007.listing__country_latest
            , nr_subq_28007.listing__is_lux_latest
            , nr_subq_28007.listing__capacity_latest
            , nr_subq_28007.listings
            , nr_subq_28007.largest_listing
            , nr_subq_28007.smallest_listing
          FROM (
            -- Read Elements From Semantic Model 'listings_latest'
            SELECT
              1 AS listings
              , listings_latest_src_28000.capacity AS largest_listing
              , listings_latest_src_28000.capacity AS smallest_listing
              , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS ds__day
              , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS ds__week
              , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS ds__month
              , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS ds__quarter
              , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS ds__year
              , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
              , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
              , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
              , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
              , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS ds__extract_dow
              , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS ds__extract_doy
              , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS created_at__day
              , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS created_at__week
              , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS created_at__month
              , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS created_at__quarter
              , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS created_at__year
              , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
              , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
              , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
              , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
              , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
              , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
              , listings_latest_src_28000.country AS country_latest
              , listings_latest_src_28000.is_lux AS is_lux_latest
              , listings_latest_src_28000.capacity AS capacity_latest
              , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__ds__day
              , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__ds__week
              , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__ds__month
              , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__ds__quarter
              , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__ds__year
              , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
              , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
              , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
              , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
              , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
              , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
              , DATE_TRUNC('day', listings_latest_src_28000.created_at) AS listing__created_at__day
              , DATE_TRUNC('week', listings_latest_src_28000.created_at) AS listing__created_at__week
              , DATE_TRUNC('month', listings_latest_src_28000.created_at) AS listing__created_at__month
              , DATE_TRUNC('quarter', listings_latest_src_28000.created_at) AS listing__created_at__quarter
              , DATE_TRUNC('year', listings_latest_src_28000.created_at) AS listing__created_at__year
              , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
              , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
              , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
              , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
              , EXTRACT(isodow FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
              , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
              , listings_latest_src_28000.country AS listing__country_latest
              , listings_latest_src_28000.is_lux AS listing__is_lux_latest
              , listings_latest_src_28000.capacity AS listing__capacity_latest
              , listings_latest_src_28000.listing_id AS listing
              , listings_latest_src_28000.user_id AS user
              , listings_latest_src_28000.user_id AS listing__user
            FROM ***************************.dim_listings_latest listings_latest_src_28000
          ) nr_subq_28007
        ) nr_subq_12
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['user', 'user__visit_buy_conversion_rate']
          SELECT
            nr_subq_25.user
            , nr_subq_25.user__visit_buy_conversion_rate
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              nr_subq_24.user
              , CAST(nr_subq_24.buys AS DOUBLE PRECISION) / CAST(NULLIF(nr_subq_24.visits, 0) AS DOUBLE PRECISION) AS user__visit_buy_conversion_rate
            FROM (
              -- Combine Aggregated Outputs
              SELECT
                COALESCE(nr_subq_15.user, nr_subq_23.user) AS user
                , MAX(nr_subq_15.visits) AS visits
                , MAX(nr_subq_23.buys) AS buys
              FROM (
                -- Aggregate Measures
                SELECT
                  nr_subq_14.user
                  , SUM(nr_subq_14.visits) AS visits
                FROM (
                  -- Pass Only Elements: ['visits', 'user']
                  SELECT
                    nr_subq_13.user
                    , nr_subq_13.visits
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      nr_subq_28012.ds__day
                      , nr_subq_28012.ds__week
                      , nr_subq_28012.ds__month
                      , nr_subq_28012.ds__quarter
                      , nr_subq_28012.ds__year
                      , nr_subq_28012.ds__extract_year
                      , nr_subq_28012.ds__extract_quarter
                      , nr_subq_28012.ds__extract_month
                      , nr_subq_28012.ds__extract_day
                      , nr_subq_28012.ds__extract_dow
                      , nr_subq_28012.ds__extract_doy
                      , nr_subq_28012.visit__ds__day
                      , nr_subq_28012.visit__ds__week
                      , nr_subq_28012.visit__ds__month
                      , nr_subq_28012.visit__ds__quarter
                      , nr_subq_28012.visit__ds__year
                      , nr_subq_28012.visit__ds__extract_year
                      , nr_subq_28012.visit__ds__extract_quarter
                      , nr_subq_28012.visit__ds__extract_month
                      , nr_subq_28012.visit__ds__extract_day
                      , nr_subq_28012.visit__ds__extract_dow
                      , nr_subq_28012.visit__ds__extract_doy
                      , nr_subq_28012.ds__day AS metric_time__day
                      , nr_subq_28012.ds__week AS metric_time__week
                      , nr_subq_28012.ds__month AS metric_time__month
                      , nr_subq_28012.ds__quarter AS metric_time__quarter
                      , nr_subq_28012.ds__year AS metric_time__year
                      , nr_subq_28012.ds__extract_year AS metric_time__extract_year
                      , nr_subq_28012.ds__extract_quarter AS metric_time__extract_quarter
                      , nr_subq_28012.ds__extract_month AS metric_time__extract_month
                      , nr_subq_28012.ds__extract_day AS metric_time__extract_day
                      , nr_subq_28012.ds__extract_dow AS metric_time__extract_dow
                      , nr_subq_28012.ds__extract_doy AS metric_time__extract_doy
                      , nr_subq_28012.user
                      , nr_subq_28012.session
                      , nr_subq_28012.visit__user
                      , nr_subq_28012.visit__session
                      , nr_subq_28012.referrer_id
                      , nr_subq_28012.visit__referrer_id
                      , nr_subq_28012.visits
                      , nr_subq_28012.visitors
                    FROM (
                      -- Read Elements From Semantic Model 'visits_source'
                      SELECT
                        1 AS visits
                        , visits_source_src_28000.user_id AS visitors
                        , DATE_TRUNC('day', visits_source_src_28000.ds) AS ds__day
                        , DATE_TRUNC('week', visits_source_src_28000.ds) AS ds__week
                        , DATE_TRUNC('month', visits_source_src_28000.ds) AS ds__month
                        , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS ds__quarter
                        , DATE_TRUNC('year', visits_source_src_28000.ds) AS ds__year
                        , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
                        , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
                        , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
                        , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
                        , EXTRACT(isodow FROM visits_source_src_28000.ds) AS ds__extract_dow
                        , EXTRACT(doy FROM visits_source_src_28000.ds) AS ds__extract_doy
                        , visits_source_src_28000.referrer_id
                        , DATE_TRUNC('day', visits_source_src_28000.ds) AS visit__ds__day
                        , DATE_TRUNC('week', visits_source_src_28000.ds) AS visit__ds__week
                        , DATE_TRUNC('month', visits_source_src_28000.ds) AS visit__ds__month
                        , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS visit__ds__quarter
                        , DATE_TRUNC('year', visits_source_src_28000.ds) AS visit__ds__year
                        , EXTRACT(year FROM visits_source_src_28000.ds) AS visit__ds__extract_year
                        , EXTRACT(quarter FROM visits_source_src_28000.ds) AS visit__ds__extract_quarter
                        , EXTRACT(month FROM visits_source_src_28000.ds) AS visit__ds__extract_month
                        , EXTRACT(day FROM visits_source_src_28000.ds) AS visit__ds__extract_day
                        , EXTRACT(isodow FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                        , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                        , visits_source_src_28000.referrer_id AS visit__referrer_id
                        , visits_source_src_28000.user_id AS user
                        , visits_source_src_28000.session_id AS session
                        , visits_source_src_28000.user_id AS visit__user
                        , visits_source_src_28000.session_id AS visit__session
                      FROM ***************************.fct_visits visits_source_src_28000
                    ) nr_subq_28012
                  ) nr_subq_13
                ) nr_subq_14
                GROUP BY
                  nr_subq_14.user
              ) nr_subq_15
              FULL OUTER JOIN (
                -- Aggregate Measures
                SELECT
                  nr_subq_22.user
                  , SUM(nr_subq_22.buys) AS buys
                FROM (
                  -- Pass Only Elements: ['buys', 'user']
                  SELECT
                    nr_subq_21.user
                    , nr_subq_21.buys
                  FROM (
                    -- Find conversions for user within the range of INF
                    SELECT
                      nr_subq_20.metric_time__day
                      , nr_subq_20.user
                      , nr_subq_20.buys
                      , nr_subq_20.visits
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
                        -- Pass Only Elements: ['visits', 'metric_time__day', 'user']
                        SELECT
                          nr_subq_16.metric_time__day
                          , nr_subq_16.user
                          , nr_subq_16.visits
                        FROM (
                          -- Metric Time Dimension 'ds'
                          SELECT
                            nr_subq_28012.ds__day
                            , nr_subq_28012.ds__week
                            , nr_subq_28012.ds__month
                            , nr_subq_28012.ds__quarter
                            , nr_subq_28012.ds__year
                            , nr_subq_28012.ds__extract_year
                            , nr_subq_28012.ds__extract_quarter
                            , nr_subq_28012.ds__extract_month
                            , nr_subq_28012.ds__extract_day
                            , nr_subq_28012.ds__extract_dow
                            , nr_subq_28012.ds__extract_doy
                            , nr_subq_28012.visit__ds__day
                            , nr_subq_28012.visit__ds__week
                            , nr_subq_28012.visit__ds__month
                            , nr_subq_28012.visit__ds__quarter
                            , nr_subq_28012.visit__ds__year
                            , nr_subq_28012.visit__ds__extract_year
                            , nr_subq_28012.visit__ds__extract_quarter
                            , nr_subq_28012.visit__ds__extract_month
                            , nr_subq_28012.visit__ds__extract_day
                            , nr_subq_28012.visit__ds__extract_dow
                            , nr_subq_28012.visit__ds__extract_doy
                            , nr_subq_28012.ds__day AS metric_time__day
                            , nr_subq_28012.ds__week AS metric_time__week
                            , nr_subq_28012.ds__month AS metric_time__month
                            , nr_subq_28012.ds__quarter AS metric_time__quarter
                            , nr_subq_28012.ds__year AS metric_time__year
                            , nr_subq_28012.ds__extract_year AS metric_time__extract_year
                            , nr_subq_28012.ds__extract_quarter AS metric_time__extract_quarter
                            , nr_subq_28012.ds__extract_month AS metric_time__extract_month
                            , nr_subq_28012.ds__extract_day AS metric_time__extract_day
                            , nr_subq_28012.ds__extract_dow AS metric_time__extract_dow
                            , nr_subq_28012.ds__extract_doy AS metric_time__extract_doy
                            , nr_subq_28012.user
                            , nr_subq_28012.session
                            , nr_subq_28012.visit__user
                            , nr_subq_28012.visit__session
                            , nr_subq_28012.referrer_id
                            , nr_subq_28012.visit__referrer_id
                            , nr_subq_28012.visits
                            , nr_subq_28012.visitors
                          FROM (
                            -- Read Elements From Semantic Model 'visits_source'
                            SELECT
                              1 AS visits
                              , visits_source_src_28000.user_id AS visitors
                              , DATE_TRUNC('day', visits_source_src_28000.ds) AS ds__day
                              , DATE_TRUNC('week', visits_source_src_28000.ds) AS ds__week
                              , DATE_TRUNC('month', visits_source_src_28000.ds) AS ds__month
                              , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS ds__quarter
                              , DATE_TRUNC('year', visits_source_src_28000.ds) AS ds__year
                              , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
                              , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
                              , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
                              , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
                              , EXTRACT(isodow FROM visits_source_src_28000.ds) AS ds__extract_dow
                              , EXTRACT(doy FROM visits_source_src_28000.ds) AS ds__extract_doy
                              , visits_source_src_28000.referrer_id
                              , DATE_TRUNC('day', visits_source_src_28000.ds) AS visit__ds__day
                              , DATE_TRUNC('week', visits_source_src_28000.ds) AS visit__ds__week
                              , DATE_TRUNC('month', visits_source_src_28000.ds) AS visit__ds__month
                              , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS visit__ds__quarter
                              , DATE_TRUNC('year', visits_source_src_28000.ds) AS visit__ds__year
                              , EXTRACT(year FROM visits_source_src_28000.ds) AS visit__ds__extract_year
                              , EXTRACT(quarter FROM visits_source_src_28000.ds) AS visit__ds__extract_quarter
                              , EXTRACT(month FROM visits_source_src_28000.ds) AS visit__ds__extract_month
                              , EXTRACT(day FROM visits_source_src_28000.ds) AS visit__ds__extract_day
                              , EXTRACT(isodow FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                              , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                              , visits_source_src_28000.referrer_id AS visit__referrer_id
                              , visits_source_src_28000.user_id AS user
                              , visits_source_src_28000.session_id AS session
                              , visits_source_src_28000.user_id AS visit__user
                              , visits_source_src_28000.session_id AS visit__session
                            FROM ***************************.fct_visits visits_source_src_28000
                          ) nr_subq_28012
                        ) nr_subq_16
                      ) nr_subq_17
                      INNER JOIN (
                        -- Add column with generated UUID
                        SELECT
                          nr_subq_18.ds__day
                          , nr_subq_18.ds__week
                          , nr_subq_18.ds__month
                          , nr_subq_18.ds__quarter
                          , nr_subq_18.ds__year
                          , nr_subq_18.ds__extract_year
                          , nr_subq_18.ds__extract_quarter
                          , nr_subq_18.ds__extract_month
                          , nr_subq_18.ds__extract_day
                          , nr_subq_18.ds__extract_dow
                          , nr_subq_18.ds__extract_doy
                          , nr_subq_18.ds_month__month
                          , nr_subq_18.ds_month__quarter
                          , nr_subq_18.ds_month__year
                          , nr_subq_18.ds_month__extract_year
                          , nr_subq_18.ds_month__extract_quarter
                          , nr_subq_18.ds_month__extract_month
                          , nr_subq_18.buy__ds__day
                          , nr_subq_18.buy__ds__week
                          , nr_subq_18.buy__ds__month
                          , nr_subq_18.buy__ds__quarter
                          , nr_subq_18.buy__ds__year
                          , nr_subq_18.buy__ds__extract_year
                          , nr_subq_18.buy__ds__extract_quarter
                          , nr_subq_18.buy__ds__extract_month
                          , nr_subq_18.buy__ds__extract_day
                          , nr_subq_18.buy__ds__extract_dow
                          , nr_subq_18.buy__ds__extract_doy
                          , nr_subq_18.buy__ds_month__month
                          , nr_subq_18.buy__ds_month__quarter
                          , nr_subq_18.buy__ds_month__year
                          , nr_subq_18.buy__ds_month__extract_year
                          , nr_subq_18.buy__ds_month__extract_quarter
                          , nr_subq_18.buy__ds_month__extract_month
                          , nr_subq_18.metric_time__day
                          , nr_subq_18.metric_time__week
                          , nr_subq_18.metric_time__month
                          , nr_subq_18.metric_time__quarter
                          , nr_subq_18.metric_time__year
                          , nr_subq_18.metric_time__extract_year
                          , nr_subq_18.metric_time__extract_quarter
                          , nr_subq_18.metric_time__extract_month
                          , nr_subq_18.metric_time__extract_day
                          , nr_subq_18.metric_time__extract_dow
                          , nr_subq_18.metric_time__extract_doy
                          , nr_subq_18.user
                          , nr_subq_18.session_id
                          , nr_subq_18.buy__user
                          , nr_subq_18.buy__session_id
                          , nr_subq_18.buys
                          , nr_subq_18.buyers
                          , GEN_RANDOM_UUID() AS mf_internal_uuid
                        FROM (
                          -- Metric Time Dimension 'ds'
                          SELECT
                            nr_subq_28004.ds__day
                            , nr_subq_28004.ds__week
                            , nr_subq_28004.ds__month
                            , nr_subq_28004.ds__quarter
                            , nr_subq_28004.ds__year
                            , nr_subq_28004.ds__extract_year
                            , nr_subq_28004.ds__extract_quarter
                            , nr_subq_28004.ds__extract_month
                            , nr_subq_28004.ds__extract_day
                            , nr_subq_28004.ds__extract_dow
                            , nr_subq_28004.ds__extract_doy
                            , nr_subq_28004.ds_month__month
                            , nr_subq_28004.ds_month__quarter
                            , nr_subq_28004.ds_month__year
                            , nr_subq_28004.ds_month__extract_year
                            , nr_subq_28004.ds_month__extract_quarter
                            , nr_subq_28004.ds_month__extract_month
                            , nr_subq_28004.buy__ds__day
                            , nr_subq_28004.buy__ds__week
                            , nr_subq_28004.buy__ds__month
                            , nr_subq_28004.buy__ds__quarter
                            , nr_subq_28004.buy__ds__year
                            , nr_subq_28004.buy__ds__extract_year
                            , nr_subq_28004.buy__ds__extract_quarter
                            , nr_subq_28004.buy__ds__extract_month
                            , nr_subq_28004.buy__ds__extract_day
                            , nr_subq_28004.buy__ds__extract_dow
                            , nr_subq_28004.buy__ds__extract_doy
                            , nr_subq_28004.buy__ds_month__month
                            , nr_subq_28004.buy__ds_month__quarter
                            , nr_subq_28004.buy__ds_month__year
                            , nr_subq_28004.buy__ds_month__extract_year
                            , nr_subq_28004.buy__ds_month__extract_quarter
                            , nr_subq_28004.buy__ds_month__extract_month
                            , nr_subq_28004.ds__day AS metric_time__day
                            , nr_subq_28004.ds__week AS metric_time__week
                            , nr_subq_28004.ds__month AS metric_time__month
                            , nr_subq_28004.ds__quarter AS metric_time__quarter
                            , nr_subq_28004.ds__year AS metric_time__year
                            , nr_subq_28004.ds__extract_year AS metric_time__extract_year
                            , nr_subq_28004.ds__extract_quarter AS metric_time__extract_quarter
                            , nr_subq_28004.ds__extract_month AS metric_time__extract_month
                            , nr_subq_28004.ds__extract_day AS metric_time__extract_day
                            , nr_subq_28004.ds__extract_dow AS metric_time__extract_dow
                            , nr_subq_28004.ds__extract_doy AS metric_time__extract_doy
                            , nr_subq_28004.user
                            , nr_subq_28004.session_id
                            , nr_subq_28004.buy__user
                            , nr_subq_28004.buy__session_id
                            , nr_subq_28004.buys
                            , nr_subq_28004.buyers
                          FROM (
                            -- Read Elements From Semantic Model 'buys_source'
                            SELECT
                              1 AS buys
                              , 1 AS buys_month
                              , buys_source_src_28000.user_id AS buyers
                              , DATE_TRUNC('day', buys_source_src_28000.ds) AS ds__day
                              , DATE_TRUNC('week', buys_source_src_28000.ds) AS ds__week
                              , DATE_TRUNC('month', buys_source_src_28000.ds) AS ds__month
                              , DATE_TRUNC('quarter', buys_source_src_28000.ds) AS ds__quarter
                              , DATE_TRUNC('year', buys_source_src_28000.ds) AS ds__year
                              , EXTRACT(year FROM buys_source_src_28000.ds) AS ds__extract_year
                              , EXTRACT(quarter FROM buys_source_src_28000.ds) AS ds__extract_quarter
                              , EXTRACT(month FROM buys_source_src_28000.ds) AS ds__extract_month
                              , EXTRACT(day FROM buys_source_src_28000.ds) AS ds__extract_day
                              , EXTRACT(isodow FROM buys_source_src_28000.ds) AS ds__extract_dow
                              , EXTRACT(doy FROM buys_source_src_28000.ds) AS ds__extract_doy
                              , DATE_TRUNC('month', buys_source_src_28000.ds_month) AS ds_month__month
                              , DATE_TRUNC('quarter', buys_source_src_28000.ds_month) AS ds_month__quarter
                              , DATE_TRUNC('year', buys_source_src_28000.ds_month) AS ds_month__year
                              , EXTRACT(year FROM buys_source_src_28000.ds_month) AS ds_month__extract_year
                              , EXTRACT(quarter FROM buys_source_src_28000.ds_month) AS ds_month__extract_quarter
                              , EXTRACT(month FROM buys_source_src_28000.ds_month) AS ds_month__extract_month
                              , DATE_TRUNC('day', buys_source_src_28000.ds) AS buy__ds__day
                              , DATE_TRUNC('week', buys_source_src_28000.ds) AS buy__ds__week
                              , DATE_TRUNC('month', buys_source_src_28000.ds) AS buy__ds__month
                              , DATE_TRUNC('quarter', buys_source_src_28000.ds) AS buy__ds__quarter
                              , DATE_TRUNC('year', buys_source_src_28000.ds) AS buy__ds__year
                              , EXTRACT(year FROM buys_source_src_28000.ds) AS buy__ds__extract_year
                              , EXTRACT(quarter FROM buys_source_src_28000.ds) AS buy__ds__extract_quarter
                              , EXTRACT(month FROM buys_source_src_28000.ds) AS buy__ds__extract_month
                              , EXTRACT(day FROM buys_source_src_28000.ds) AS buy__ds__extract_day
                              , EXTRACT(isodow FROM buys_source_src_28000.ds) AS buy__ds__extract_dow
                              , EXTRACT(doy FROM buys_source_src_28000.ds) AS buy__ds__extract_doy
                              , DATE_TRUNC('month', buys_source_src_28000.ds_month) AS buy__ds_month__month
                              , DATE_TRUNC('quarter', buys_source_src_28000.ds_month) AS buy__ds_month__quarter
                              , DATE_TRUNC('year', buys_source_src_28000.ds_month) AS buy__ds_month__year
                              , EXTRACT(year FROM buys_source_src_28000.ds_month) AS buy__ds_month__extract_year
                              , EXTRACT(quarter FROM buys_source_src_28000.ds_month) AS buy__ds_month__extract_quarter
                              , EXTRACT(month FROM buys_source_src_28000.ds_month) AS buy__ds_month__extract_month
                              , buys_source_src_28000.user_id AS user
                              , buys_source_src_28000.session_id
                              , buys_source_src_28000.user_id AS buy__user
                              , buys_source_src_28000.session_id AS buy__session_id
                            FROM ***************************.fct_buys buys_source_src_28000
                          ) nr_subq_28004
                        ) nr_subq_18
                      ) nr_subq_19
                      ON
                        (
                          nr_subq_17.user = nr_subq_19.user
                        ) AND (
                          (nr_subq_17.metric_time__day <= nr_subq_19.metric_time__day)
                        )
                    ) nr_subq_20
                  ) nr_subq_21
                ) nr_subq_22
                GROUP BY
                  nr_subq_22.user
              ) nr_subq_23
              ON
                nr_subq_15.user = nr_subq_23.user
              GROUP BY
                COALESCE(nr_subq_15.user, nr_subq_23.user)
            ) nr_subq_24
          ) nr_subq_25
        ) nr_subq_26
        ON
          nr_subq_12.user = nr_subq_26.user
      ) nr_subq_27
      WHERE user__visit_buy_conversion_rate > 2
    ) nr_subq_28
  ) nr_subq_29
) nr_subq_30
