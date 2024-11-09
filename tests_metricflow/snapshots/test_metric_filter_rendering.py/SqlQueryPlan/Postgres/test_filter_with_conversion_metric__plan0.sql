test_name: test_filter_with_conversion_metric
test_filename: test_metric_filter_rendering.py
---
-- Compute Metrics via Expressions
SELECT
  subq_22.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_21.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_20.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_19.ds__day
        , subq_19.ds__week
        , subq_19.ds__month
        , subq_19.ds__quarter
        , subq_19.ds__year
        , subq_19.ds__extract_year
        , subq_19.ds__extract_quarter
        , subq_19.ds__extract_month
        , subq_19.ds__extract_day
        , subq_19.ds__extract_dow
        , subq_19.ds__extract_doy
        , subq_19.created_at__day
        , subq_19.created_at__week
        , subq_19.created_at__month
        , subq_19.created_at__quarter
        , subq_19.created_at__year
        , subq_19.created_at__extract_year
        , subq_19.created_at__extract_quarter
        , subq_19.created_at__extract_month
        , subq_19.created_at__extract_day
        , subq_19.created_at__extract_dow
        , subq_19.created_at__extract_doy
        , subq_19.listing__ds__day
        , subq_19.listing__ds__week
        , subq_19.listing__ds__month
        , subq_19.listing__ds__quarter
        , subq_19.listing__ds__year
        , subq_19.listing__ds__extract_year
        , subq_19.listing__ds__extract_quarter
        , subq_19.listing__ds__extract_month
        , subq_19.listing__ds__extract_day
        , subq_19.listing__ds__extract_dow
        , subq_19.listing__ds__extract_doy
        , subq_19.listing__created_at__day
        , subq_19.listing__created_at__week
        , subq_19.listing__created_at__month
        , subq_19.listing__created_at__quarter
        , subq_19.listing__created_at__year
        , subq_19.listing__created_at__extract_year
        , subq_19.listing__created_at__extract_quarter
        , subq_19.listing__created_at__extract_month
        , subq_19.listing__created_at__extract_day
        , subq_19.listing__created_at__extract_dow
        , subq_19.listing__created_at__extract_doy
        , subq_19.metric_time__day
        , subq_19.metric_time__week
        , subq_19.metric_time__month
        , subq_19.metric_time__quarter
        , subq_19.metric_time__year
        , subq_19.metric_time__extract_year
        , subq_19.metric_time__extract_quarter
        , subq_19.metric_time__extract_month
        , subq_19.metric_time__extract_day
        , subq_19.metric_time__extract_dow
        , subq_19.metric_time__extract_doy
        , subq_19.listing
        , subq_19.user
        , subq_19.listing__user
        , subq_19.country_latest
        , subq_19.is_lux_latest
        , subq_19.capacity_latest
        , subq_19.listing__country_latest
        , subq_19.listing__is_lux_latest
        , subq_19.listing__capacity_latest
        , subq_19.user__visit_buy_conversion_rate
        , subq_19.listings
        , subq_19.largest_listing
        , subq_19.smallest_listing
      FROM (
        -- Join Standard Outputs
        SELECT
          subq_18.user__visit_buy_conversion_rate AS user__visit_buy_conversion_rate
          , subq_1.ds__day AS ds__day
          , subq_1.ds__week AS ds__week
          , subq_1.ds__month AS ds__month
          , subq_1.ds__quarter AS ds__quarter
          , subq_1.ds__year AS ds__year
          , subq_1.ds__extract_year AS ds__extract_year
          , subq_1.ds__extract_quarter AS ds__extract_quarter
          , subq_1.ds__extract_month AS ds__extract_month
          , subq_1.ds__extract_day AS ds__extract_day
          , subq_1.ds__extract_dow AS ds__extract_dow
          , subq_1.ds__extract_doy AS ds__extract_doy
          , subq_1.created_at__day AS created_at__day
          , subq_1.created_at__week AS created_at__week
          , subq_1.created_at__month AS created_at__month
          , subq_1.created_at__quarter AS created_at__quarter
          , subq_1.created_at__year AS created_at__year
          , subq_1.created_at__extract_year AS created_at__extract_year
          , subq_1.created_at__extract_quarter AS created_at__extract_quarter
          , subq_1.created_at__extract_month AS created_at__extract_month
          , subq_1.created_at__extract_day AS created_at__extract_day
          , subq_1.created_at__extract_dow AS created_at__extract_dow
          , subq_1.created_at__extract_doy AS created_at__extract_doy
          , subq_1.listing__ds__day AS listing__ds__day
          , subq_1.listing__ds__week AS listing__ds__week
          , subq_1.listing__ds__month AS listing__ds__month
          , subq_1.listing__ds__quarter AS listing__ds__quarter
          , subq_1.listing__ds__year AS listing__ds__year
          , subq_1.listing__ds__extract_year AS listing__ds__extract_year
          , subq_1.listing__ds__extract_quarter AS listing__ds__extract_quarter
          , subq_1.listing__ds__extract_month AS listing__ds__extract_month
          , subq_1.listing__ds__extract_day AS listing__ds__extract_day
          , subq_1.listing__ds__extract_dow AS listing__ds__extract_dow
          , subq_1.listing__ds__extract_doy AS listing__ds__extract_doy
          , subq_1.listing__created_at__day AS listing__created_at__day
          , subq_1.listing__created_at__week AS listing__created_at__week
          , subq_1.listing__created_at__month AS listing__created_at__month
          , subq_1.listing__created_at__quarter AS listing__created_at__quarter
          , subq_1.listing__created_at__year AS listing__created_at__year
          , subq_1.listing__created_at__extract_year AS listing__created_at__extract_year
          , subq_1.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
          , subq_1.listing__created_at__extract_month AS listing__created_at__extract_month
          , subq_1.listing__created_at__extract_day AS listing__created_at__extract_day
          , subq_1.listing__created_at__extract_dow AS listing__created_at__extract_dow
          , subq_1.listing__created_at__extract_doy AS listing__created_at__extract_doy
          , subq_1.metric_time__day AS metric_time__day
          , subq_1.metric_time__week AS metric_time__week
          , subq_1.metric_time__month AS metric_time__month
          , subq_1.metric_time__quarter AS metric_time__quarter
          , subq_1.metric_time__year AS metric_time__year
          , subq_1.metric_time__extract_year AS metric_time__extract_year
          , subq_1.metric_time__extract_quarter AS metric_time__extract_quarter
          , subq_1.metric_time__extract_month AS metric_time__extract_month
          , subq_1.metric_time__extract_day AS metric_time__extract_day
          , subq_1.metric_time__extract_dow AS metric_time__extract_dow
          , subq_1.metric_time__extract_doy AS metric_time__extract_doy
          , subq_1.listing AS listing
          , subq_1.user AS user
          , subq_1.listing__user AS listing__user
          , subq_1.country_latest AS country_latest
          , subq_1.is_lux_latest AS is_lux_latest
          , subq_1.capacity_latest AS capacity_latest
          , subq_1.listing__country_latest AS listing__country_latest
          , subq_1.listing__is_lux_latest AS listing__is_lux_latest
          , subq_1.listing__capacity_latest AS listing__capacity_latest
          , subq_1.listings AS listings
          , subq_1.largest_listing AS largest_listing
          , subq_1.smallest_listing AS smallest_listing
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_0.ds__day
            , subq_0.ds__week
            , subq_0.ds__month
            , subq_0.ds__quarter
            , subq_0.ds__year
            , subq_0.ds__extract_year
            , subq_0.ds__extract_quarter
            , subq_0.ds__extract_month
            , subq_0.ds__extract_day
            , subq_0.ds__extract_dow
            , subq_0.ds__extract_doy
            , subq_0.created_at__day
            , subq_0.created_at__week
            , subq_0.created_at__month
            , subq_0.created_at__quarter
            , subq_0.created_at__year
            , subq_0.created_at__extract_year
            , subq_0.created_at__extract_quarter
            , subq_0.created_at__extract_month
            , subq_0.created_at__extract_day
            , subq_0.created_at__extract_dow
            , subq_0.created_at__extract_doy
            , subq_0.listing__ds__day
            , subq_0.listing__ds__week
            , subq_0.listing__ds__month
            , subq_0.listing__ds__quarter
            , subq_0.listing__ds__year
            , subq_0.listing__ds__extract_year
            , subq_0.listing__ds__extract_quarter
            , subq_0.listing__ds__extract_month
            , subq_0.listing__ds__extract_day
            , subq_0.listing__ds__extract_dow
            , subq_0.listing__ds__extract_doy
            , subq_0.listing__created_at__day
            , subq_0.listing__created_at__week
            , subq_0.listing__created_at__month
            , subq_0.listing__created_at__quarter
            , subq_0.listing__created_at__year
            , subq_0.listing__created_at__extract_year
            , subq_0.listing__created_at__extract_quarter
            , subq_0.listing__created_at__extract_month
            , subq_0.listing__created_at__extract_day
            , subq_0.listing__created_at__extract_dow
            , subq_0.listing__created_at__extract_doy
            , subq_0.ds__day AS metric_time__day
            , subq_0.ds__week AS metric_time__week
            , subq_0.ds__month AS metric_time__month
            , subq_0.ds__quarter AS metric_time__quarter
            , subq_0.ds__year AS metric_time__year
            , subq_0.ds__extract_year AS metric_time__extract_year
            , subq_0.ds__extract_quarter AS metric_time__extract_quarter
            , subq_0.ds__extract_month AS metric_time__extract_month
            , subq_0.ds__extract_day AS metric_time__extract_day
            , subq_0.ds__extract_dow AS metric_time__extract_dow
            , subq_0.ds__extract_doy AS metric_time__extract_doy
            , subq_0.listing
            , subq_0.user
            , subq_0.listing__user
            , subq_0.country_latest
            , subq_0.is_lux_latest
            , subq_0.capacity_latest
            , subq_0.listing__country_latest
            , subq_0.listing__is_lux_latest
            , subq_0.listing__capacity_latest
            , subq_0.listings
            , subq_0.largest_listing
            , subq_0.smallest_listing
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
          ) subq_0
        ) subq_1
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['user', 'user__visit_buy_conversion_rate']
          SELECT
            subq_17.user
            , subq_17.user__visit_buy_conversion_rate
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              subq_16.user
              , CAST(subq_16.buys AS DOUBLE PRECISION) / CAST(NULLIF(subq_16.visits, 0) AS DOUBLE PRECISION) AS user__visit_buy_conversion_rate
            FROM (
              -- Combine Aggregated Outputs
              SELECT
                COALESCE(subq_5.user, subq_15.user) AS user
                , MAX(subq_5.visits) AS visits
                , MAX(subq_15.buys) AS buys
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_4.user
                  , SUM(subq_4.visits) AS visits
                FROM (
                  -- Pass Only Elements: ['visits', 'user']
                  SELECT
                    subq_3.user
                    , subq_3.visits
                  FROM (
                    -- Metric Time Dimension 'ds'
                    SELECT
                      subq_2.ds__day
                      , subq_2.ds__week
                      , subq_2.ds__month
                      , subq_2.ds__quarter
                      , subq_2.ds__year
                      , subq_2.ds__extract_year
                      , subq_2.ds__extract_quarter
                      , subq_2.ds__extract_month
                      , subq_2.ds__extract_day
                      , subq_2.ds__extract_dow
                      , subq_2.ds__extract_doy
                      , subq_2.visit__ds__day
                      , subq_2.visit__ds__week
                      , subq_2.visit__ds__month
                      , subq_2.visit__ds__quarter
                      , subq_2.visit__ds__year
                      , subq_2.visit__ds__extract_year
                      , subq_2.visit__ds__extract_quarter
                      , subq_2.visit__ds__extract_month
                      , subq_2.visit__ds__extract_day
                      , subq_2.visit__ds__extract_dow
                      , subq_2.visit__ds__extract_doy
                      , subq_2.ds__day AS metric_time__day
                      , subq_2.ds__week AS metric_time__week
                      , subq_2.ds__month AS metric_time__month
                      , subq_2.ds__quarter AS metric_time__quarter
                      , subq_2.ds__year AS metric_time__year
                      , subq_2.ds__extract_year AS metric_time__extract_year
                      , subq_2.ds__extract_quarter AS metric_time__extract_quarter
                      , subq_2.ds__extract_month AS metric_time__extract_month
                      , subq_2.ds__extract_day AS metric_time__extract_day
                      , subq_2.ds__extract_dow AS metric_time__extract_dow
                      , subq_2.ds__extract_doy AS metric_time__extract_doy
                      , subq_2.user
                      , subq_2.session
                      , subq_2.visit__user
                      , subq_2.visit__session
                      , subq_2.referrer_id
                      , subq_2.visit__referrer_id
                      , subq_2.visits
                      , subq_2.visitors
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
                    ) subq_2
                  ) subq_3
                ) subq_4
                GROUP BY
                  subq_4.user
              ) subq_5
              FULL OUTER JOIN (
                -- Aggregate Measures
                SELECT
                  subq_14.user
                  , SUM(subq_14.buys) AS buys
                FROM (
                  -- Pass Only Elements: ['buys', 'user']
                  SELECT
                    subq_13.user
                    , subq_13.buys
                  FROM (
                    -- Find conversions for user within the range of INF
                    SELECT
                      subq_12.metric_time__day
                      , subq_12.user
                      , subq_12.buys
                      , subq_12.visits
                    FROM (
                      -- Dedupe the fanout with mf_internal_uuid in the conversion data set
                      SELECT DISTINCT
                        FIRST_VALUE(subq_8.visits) OVER (
                          PARTITION BY
                            subq_11.user
                            , subq_11.metric_time__day
                            , subq_11.mf_internal_uuid
                          ORDER BY subq_8.metric_time__day DESC
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS visits
                        , FIRST_VALUE(subq_8.metric_time__day) OVER (
                          PARTITION BY
                            subq_11.user
                            , subq_11.metric_time__day
                            , subq_11.mf_internal_uuid
                          ORDER BY subq_8.metric_time__day DESC
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS metric_time__day
                        , FIRST_VALUE(subq_8.user) OVER (
                          PARTITION BY
                            subq_11.user
                            , subq_11.metric_time__day
                            , subq_11.mf_internal_uuid
                          ORDER BY subq_8.metric_time__day DESC
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS user
                        , subq_11.mf_internal_uuid AS mf_internal_uuid
                        , subq_11.buys AS buys
                      FROM (
                        -- Pass Only Elements: ['visits', 'metric_time__day', 'user']
                        SELECT
                          subq_7.metric_time__day
                          , subq_7.user
                          , subq_7.visits
                        FROM (
                          -- Metric Time Dimension 'ds'
                          SELECT
                            subq_6.ds__day
                            , subq_6.ds__week
                            , subq_6.ds__month
                            , subq_6.ds__quarter
                            , subq_6.ds__year
                            , subq_6.ds__extract_year
                            , subq_6.ds__extract_quarter
                            , subq_6.ds__extract_month
                            , subq_6.ds__extract_day
                            , subq_6.ds__extract_dow
                            , subq_6.ds__extract_doy
                            , subq_6.visit__ds__day
                            , subq_6.visit__ds__week
                            , subq_6.visit__ds__month
                            , subq_6.visit__ds__quarter
                            , subq_6.visit__ds__year
                            , subq_6.visit__ds__extract_year
                            , subq_6.visit__ds__extract_quarter
                            , subq_6.visit__ds__extract_month
                            , subq_6.visit__ds__extract_day
                            , subq_6.visit__ds__extract_dow
                            , subq_6.visit__ds__extract_doy
                            , subq_6.ds__day AS metric_time__day
                            , subq_6.ds__week AS metric_time__week
                            , subq_6.ds__month AS metric_time__month
                            , subq_6.ds__quarter AS metric_time__quarter
                            , subq_6.ds__year AS metric_time__year
                            , subq_6.ds__extract_year AS metric_time__extract_year
                            , subq_6.ds__extract_quarter AS metric_time__extract_quarter
                            , subq_6.ds__extract_month AS metric_time__extract_month
                            , subq_6.ds__extract_day AS metric_time__extract_day
                            , subq_6.ds__extract_dow AS metric_time__extract_dow
                            , subq_6.ds__extract_doy AS metric_time__extract_doy
                            , subq_6.user
                            , subq_6.session
                            , subq_6.visit__user
                            , subq_6.visit__session
                            , subq_6.referrer_id
                            , subq_6.visit__referrer_id
                            , subq_6.visits
                            , subq_6.visitors
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
                          ) subq_6
                        ) subq_7
                      ) subq_8
                      INNER JOIN (
                        -- Add column with generated UUID
                        SELECT
                          subq_10.ds__day
                          , subq_10.ds__week
                          , subq_10.ds__month
                          , subq_10.ds__quarter
                          , subq_10.ds__year
                          , subq_10.ds__extract_year
                          , subq_10.ds__extract_quarter
                          , subq_10.ds__extract_month
                          , subq_10.ds__extract_day
                          , subq_10.ds__extract_dow
                          , subq_10.ds__extract_doy
                          , subq_10.ds_month__month
                          , subq_10.ds_month__quarter
                          , subq_10.ds_month__year
                          , subq_10.ds_month__extract_year
                          , subq_10.ds_month__extract_quarter
                          , subq_10.ds_month__extract_month
                          , subq_10.buy__ds__day
                          , subq_10.buy__ds__week
                          , subq_10.buy__ds__month
                          , subq_10.buy__ds__quarter
                          , subq_10.buy__ds__year
                          , subq_10.buy__ds__extract_year
                          , subq_10.buy__ds__extract_quarter
                          , subq_10.buy__ds__extract_month
                          , subq_10.buy__ds__extract_day
                          , subq_10.buy__ds__extract_dow
                          , subq_10.buy__ds__extract_doy
                          , subq_10.buy__ds_month__month
                          , subq_10.buy__ds_month__quarter
                          , subq_10.buy__ds_month__year
                          , subq_10.buy__ds_month__extract_year
                          , subq_10.buy__ds_month__extract_quarter
                          , subq_10.buy__ds_month__extract_month
                          , subq_10.metric_time__day
                          , subq_10.metric_time__week
                          , subq_10.metric_time__month
                          , subq_10.metric_time__quarter
                          , subq_10.metric_time__year
                          , subq_10.metric_time__extract_year
                          , subq_10.metric_time__extract_quarter
                          , subq_10.metric_time__extract_month
                          , subq_10.metric_time__extract_day
                          , subq_10.metric_time__extract_dow
                          , subq_10.metric_time__extract_doy
                          , subq_10.user
                          , subq_10.session_id
                          , subq_10.buy__user
                          , subq_10.buy__session_id
                          , subq_10.buys
                          , subq_10.buyers
                          , GEN_RANDOM_UUID() AS mf_internal_uuid
                        FROM (
                          -- Metric Time Dimension 'ds'
                          SELECT
                            subq_9.ds__day
                            , subq_9.ds__week
                            , subq_9.ds__month
                            , subq_9.ds__quarter
                            , subq_9.ds__year
                            , subq_9.ds__extract_year
                            , subq_9.ds__extract_quarter
                            , subq_9.ds__extract_month
                            , subq_9.ds__extract_day
                            , subq_9.ds__extract_dow
                            , subq_9.ds__extract_doy
                            , subq_9.ds_month__month
                            , subq_9.ds_month__quarter
                            , subq_9.ds_month__year
                            , subq_9.ds_month__extract_year
                            , subq_9.ds_month__extract_quarter
                            , subq_9.ds_month__extract_month
                            , subq_9.buy__ds__day
                            , subq_9.buy__ds__week
                            , subq_9.buy__ds__month
                            , subq_9.buy__ds__quarter
                            , subq_9.buy__ds__year
                            , subq_9.buy__ds__extract_year
                            , subq_9.buy__ds__extract_quarter
                            , subq_9.buy__ds__extract_month
                            , subq_9.buy__ds__extract_day
                            , subq_9.buy__ds__extract_dow
                            , subq_9.buy__ds__extract_doy
                            , subq_9.buy__ds_month__month
                            , subq_9.buy__ds_month__quarter
                            , subq_9.buy__ds_month__year
                            , subq_9.buy__ds_month__extract_year
                            , subq_9.buy__ds_month__extract_quarter
                            , subq_9.buy__ds_month__extract_month
                            , subq_9.ds__day AS metric_time__day
                            , subq_9.ds__week AS metric_time__week
                            , subq_9.ds__month AS metric_time__month
                            , subq_9.ds__quarter AS metric_time__quarter
                            , subq_9.ds__year AS metric_time__year
                            , subq_9.ds__extract_year AS metric_time__extract_year
                            , subq_9.ds__extract_quarter AS metric_time__extract_quarter
                            , subq_9.ds__extract_month AS metric_time__extract_month
                            , subq_9.ds__extract_day AS metric_time__extract_day
                            , subq_9.ds__extract_dow AS metric_time__extract_dow
                            , subq_9.ds__extract_doy AS metric_time__extract_doy
                            , subq_9.user
                            , subq_9.session_id
                            , subq_9.buy__user
                            , subq_9.buy__session_id
                            , subq_9.buys
                            , subq_9.buyers
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
                          ) subq_9
                        ) subq_10
                      ) subq_11
                      ON
                        (
                          subq_8.user = subq_11.user
                        ) AND (
                          (subq_8.metric_time__day <= subq_11.metric_time__day)
                        )
                    ) subq_12
                  ) subq_13
                ) subq_14
                GROUP BY
                  subq_14.user
              ) subq_15
              ON
                subq_5.user = subq_15.user
              GROUP BY
                COALESCE(subq_5.user, subq_15.user)
            ) subq_16
          ) subq_17
        ) subq_18
        ON
          subq_1.user = subq_18.user
      ) subq_19
      WHERE user__visit_buy_conversion_rate > 2
    ) subq_20
  ) subq_21
) subq_22
