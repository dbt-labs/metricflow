test_name: test_filter_with_conversion_metric
test_filename: test_metric_filter_rendering.py
sql_engine: Redshift
---
-- Write to DataTable
SELECT
  subq_34.listings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_33.listings
  FROM (
    -- Aggregate Measures
    SELECT
      SUM(subq_32.listings) AS listings
    FROM (
      -- Pass Only Elements: ['listings']
      SELECT
        subq_31.listings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_30.ds__day
          , subq_30.ds__week
          , subq_30.ds__month
          , subq_30.ds__quarter
          , subq_30.ds__year
          , subq_30.ds__extract_year
          , subq_30.ds__extract_quarter
          , subq_30.ds__extract_month
          , subq_30.ds__extract_day
          , subq_30.ds__extract_dow
          , subq_30.ds__extract_doy
          , subq_30.created_at__day
          , subq_30.created_at__week
          , subq_30.created_at__month
          , subq_30.created_at__quarter
          , subq_30.created_at__year
          , subq_30.created_at__extract_year
          , subq_30.created_at__extract_quarter
          , subq_30.created_at__extract_month
          , subq_30.created_at__extract_day
          , subq_30.created_at__extract_dow
          , subq_30.created_at__extract_doy
          , subq_30.listing__ds__day
          , subq_30.listing__ds__week
          , subq_30.listing__ds__month
          , subq_30.listing__ds__quarter
          , subq_30.listing__ds__year
          , subq_30.listing__ds__extract_year
          , subq_30.listing__ds__extract_quarter
          , subq_30.listing__ds__extract_month
          , subq_30.listing__ds__extract_day
          , subq_30.listing__ds__extract_dow
          , subq_30.listing__ds__extract_doy
          , subq_30.listing__created_at__day
          , subq_30.listing__created_at__week
          , subq_30.listing__created_at__month
          , subq_30.listing__created_at__quarter
          , subq_30.listing__created_at__year
          , subq_30.listing__created_at__extract_year
          , subq_30.listing__created_at__extract_quarter
          , subq_30.listing__created_at__extract_month
          , subq_30.listing__created_at__extract_day
          , subq_30.listing__created_at__extract_dow
          , subq_30.listing__created_at__extract_doy
          , subq_30.metric_time__day
          , subq_30.metric_time__week
          , subq_30.metric_time__month
          , subq_30.metric_time__quarter
          , subq_30.metric_time__year
          , subq_30.metric_time__extract_year
          , subq_30.metric_time__extract_quarter
          , subq_30.metric_time__extract_month
          , subq_30.metric_time__extract_day
          , subq_30.metric_time__extract_dow
          , subq_30.metric_time__extract_doy
          , subq_30.listing
          , subq_30.user
          , subq_30.listing__user
          , subq_30.country_latest
          , subq_30.is_lux_latest
          , subq_30.capacity_latest
          , subq_30.listing__country_latest
          , subq_30.listing__is_lux_latest
          , subq_30.listing__capacity_latest
          , subq_30.user__visit_buy_conversion_rate
          , subq_30.listings
          , subq_30.largest_listing
          , subq_30.smallest_listing
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_29.user__visit_buy_conversion_rate AS user__visit_buy_conversion_rate
            , subq_13.ds__day AS ds__day
            , subq_13.ds__week AS ds__week
            , subq_13.ds__month AS ds__month
            , subq_13.ds__quarter AS ds__quarter
            , subq_13.ds__year AS ds__year
            , subq_13.ds__extract_year AS ds__extract_year
            , subq_13.ds__extract_quarter AS ds__extract_quarter
            , subq_13.ds__extract_month AS ds__extract_month
            , subq_13.ds__extract_day AS ds__extract_day
            , subq_13.ds__extract_dow AS ds__extract_dow
            , subq_13.ds__extract_doy AS ds__extract_doy
            , subq_13.created_at__day AS created_at__day
            , subq_13.created_at__week AS created_at__week
            , subq_13.created_at__month AS created_at__month
            , subq_13.created_at__quarter AS created_at__quarter
            , subq_13.created_at__year AS created_at__year
            , subq_13.created_at__extract_year AS created_at__extract_year
            , subq_13.created_at__extract_quarter AS created_at__extract_quarter
            , subq_13.created_at__extract_month AS created_at__extract_month
            , subq_13.created_at__extract_day AS created_at__extract_day
            , subq_13.created_at__extract_dow AS created_at__extract_dow
            , subq_13.created_at__extract_doy AS created_at__extract_doy
            , subq_13.listing__ds__day AS listing__ds__day
            , subq_13.listing__ds__week AS listing__ds__week
            , subq_13.listing__ds__month AS listing__ds__month
            , subq_13.listing__ds__quarter AS listing__ds__quarter
            , subq_13.listing__ds__year AS listing__ds__year
            , subq_13.listing__ds__extract_year AS listing__ds__extract_year
            , subq_13.listing__ds__extract_quarter AS listing__ds__extract_quarter
            , subq_13.listing__ds__extract_month AS listing__ds__extract_month
            , subq_13.listing__ds__extract_day AS listing__ds__extract_day
            , subq_13.listing__ds__extract_dow AS listing__ds__extract_dow
            , subq_13.listing__ds__extract_doy AS listing__ds__extract_doy
            , subq_13.listing__created_at__day AS listing__created_at__day
            , subq_13.listing__created_at__week AS listing__created_at__week
            , subq_13.listing__created_at__month AS listing__created_at__month
            , subq_13.listing__created_at__quarter AS listing__created_at__quarter
            , subq_13.listing__created_at__year AS listing__created_at__year
            , subq_13.listing__created_at__extract_year AS listing__created_at__extract_year
            , subq_13.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
            , subq_13.listing__created_at__extract_month AS listing__created_at__extract_month
            , subq_13.listing__created_at__extract_day AS listing__created_at__extract_day
            , subq_13.listing__created_at__extract_dow AS listing__created_at__extract_dow
            , subq_13.listing__created_at__extract_doy AS listing__created_at__extract_doy
            , subq_13.metric_time__day AS metric_time__day
            , subq_13.metric_time__week AS metric_time__week
            , subq_13.metric_time__month AS metric_time__month
            , subq_13.metric_time__quarter AS metric_time__quarter
            , subq_13.metric_time__year AS metric_time__year
            , subq_13.metric_time__extract_year AS metric_time__extract_year
            , subq_13.metric_time__extract_quarter AS metric_time__extract_quarter
            , subq_13.metric_time__extract_month AS metric_time__extract_month
            , subq_13.metric_time__extract_day AS metric_time__extract_day
            , subq_13.metric_time__extract_dow AS metric_time__extract_dow
            , subq_13.metric_time__extract_doy AS metric_time__extract_doy
            , subq_13.listing AS listing
            , subq_13.user AS user
            , subq_13.listing__user AS listing__user
            , subq_13.country_latest AS country_latest
            , subq_13.is_lux_latest AS is_lux_latest
            , subq_13.capacity_latest AS capacity_latest
            , subq_13.listing__country_latest AS listing__country_latest
            , subq_13.listing__is_lux_latest AS listing__is_lux_latest
            , subq_13.listing__capacity_latest AS listing__capacity_latest
            , subq_13.listings AS listings
            , subq_13.largest_listing AS largest_listing
            , subq_13.smallest_listing AS smallest_listing
          FROM (
            -- Metric Time Dimension 'ds'
            SELECT
              subq_12.ds__day
              , subq_12.ds__week
              , subq_12.ds__month
              , subq_12.ds__quarter
              , subq_12.ds__year
              , subq_12.ds__extract_year
              , subq_12.ds__extract_quarter
              , subq_12.ds__extract_month
              , subq_12.ds__extract_day
              , subq_12.ds__extract_dow
              , subq_12.ds__extract_doy
              , subq_12.created_at__day
              , subq_12.created_at__week
              , subq_12.created_at__month
              , subq_12.created_at__quarter
              , subq_12.created_at__year
              , subq_12.created_at__extract_year
              , subq_12.created_at__extract_quarter
              , subq_12.created_at__extract_month
              , subq_12.created_at__extract_day
              , subq_12.created_at__extract_dow
              , subq_12.created_at__extract_doy
              , subq_12.listing__ds__day
              , subq_12.listing__ds__week
              , subq_12.listing__ds__month
              , subq_12.listing__ds__quarter
              , subq_12.listing__ds__year
              , subq_12.listing__ds__extract_year
              , subq_12.listing__ds__extract_quarter
              , subq_12.listing__ds__extract_month
              , subq_12.listing__ds__extract_day
              , subq_12.listing__ds__extract_dow
              , subq_12.listing__ds__extract_doy
              , subq_12.listing__created_at__day
              , subq_12.listing__created_at__week
              , subq_12.listing__created_at__month
              , subq_12.listing__created_at__quarter
              , subq_12.listing__created_at__year
              , subq_12.listing__created_at__extract_year
              , subq_12.listing__created_at__extract_quarter
              , subq_12.listing__created_at__extract_month
              , subq_12.listing__created_at__extract_day
              , subq_12.listing__created_at__extract_dow
              , subq_12.listing__created_at__extract_doy
              , subq_12.ds__day AS metric_time__day
              , subq_12.ds__week AS metric_time__week
              , subq_12.ds__month AS metric_time__month
              , subq_12.ds__quarter AS metric_time__quarter
              , subq_12.ds__year AS metric_time__year
              , subq_12.ds__extract_year AS metric_time__extract_year
              , subq_12.ds__extract_quarter AS metric_time__extract_quarter
              , subq_12.ds__extract_month AS metric_time__extract_month
              , subq_12.ds__extract_day AS metric_time__extract_day
              , subq_12.ds__extract_dow AS metric_time__extract_dow
              , subq_12.ds__extract_doy AS metric_time__extract_doy
              , subq_12.listing
              , subq_12.user
              , subq_12.listing__user
              , subq_12.country_latest
              , subq_12.is_lux_latest
              , subq_12.capacity_latest
              , subq_12.listing__country_latest
              , subq_12.listing__is_lux_latest
              , subq_12.listing__capacity_latest
              , subq_12.listings
              , subq_12.largest_listing
              , subq_12.smallest_listing
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
                , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS ds__extract_dow
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
                , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS created_at__extract_dow
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
                , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__ds__extract_dow
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
                , CASE WHEN EXTRACT(dow FROM listings_latest_src_28000.created_at) = 0 THEN EXTRACT(dow FROM listings_latest_src_28000.created_at) + 7 ELSE EXTRACT(dow FROM listings_latest_src_28000.created_at) END AS listing__created_at__extract_dow
                , EXTRACT(doy FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
                , listings_latest_src_28000.country AS listing__country_latest
                , listings_latest_src_28000.is_lux AS listing__is_lux_latest
                , listings_latest_src_28000.capacity AS listing__capacity_latest
                , listings_latest_src_28000.listing_id AS listing
                , listings_latest_src_28000.user_id AS user
                , listings_latest_src_28000.user_id AS listing__user
              FROM ***************************.dim_listings_latest listings_latest_src_28000
            ) subq_12
          ) subq_13
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['user', 'user__visit_buy_conversion_rate']
            SELECT
              subq_28.user
              , subq_28.user__visit_buy_conversion_rate
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_27.user
                , CAST(subq_27.buys AS DOUBLE PRECISION) / CAST(NULLIF(subq_27.visits, 0) AS DOUBLE PRECISION) AS user__visit_buy_conversion_rate
              FROM (
                -- Combine Aggregated Outputs
                SELECT
                  COALESCE(subq_17.user, subq_26.user) AS user
                  , MAX(subq_17.visits) AS visits
                  , MAX(subq_26.buys) AS buys
                FROM (
                  -- Aggregate Measures
                  SELECT
                    subq_16.user
                    , SUM(subq_16.visits) AS visits
                  FROM (
                    -- Pass Only Elements: ['visits', 'user']
                    SELECT
                      subq_15.user
                      , subq_15.visits
                    FROM (
                      -- Metric Time Dimension 'ds'
                      SELECT
                        subq_14.ds__day
                        , subq_14.ds__week
                        , subq_14.ds__month
                        , subq_14.ds__quarter
                        , subq_14.ds__year
                        , subq_14.ds__extract_year
                        , subq_14.ds__extract_quarter
                        , subq_14.ds__extract_month
                        , subq_14.ds__extract_day
                        , subq_14.ds__extract_dow
                        , subq_14.ds__extract_doy
                        , subq_14.visit__ds__day
                        , subq_14.visit__ds__week
                        , subq_14.visit__ds__month
                        , subq_14.visit__ds__quarter
                        , subq_14.visit__ds__year
                        , subq_14.visit__ds__extract_year
                        , subq_14.visit__ds__extract_quarter
                        , subq_14.visit__ds__extract_month
                        , subq_14.visit__ds__extract_day
                        , subq_14.visit__ds__extract_dow
                        , subq_14.visit__ds__extract_doy
                        , subq_14.ds__day AS metric_time__day
                        , subq_14.ds__week AS metric_time__week
                        , subq_14.ds__month AS metric_time__month
                        , subq_14.ds__quarter AS metric_time__quarter
                        , subq_14.ds__year AS metric_time__year
                        , subq_14.ds__extract_year AS metric_time__extract_year
                        , subq_14.ds__extract_quarter AS metric_time__extract_quarter
                        , subq_14.ds__extract_month AS metric_time__extract_month
                        , subq_14.ds__extract_day AS metric_time__extract_day
                        , subq_14.ds__extract_dow AS metric_time__extract_dow
                        , subq_14.ds__extract_doy AS metric_time__extract_doy
                        , subq_14.user
                        , subq_14.session
                        , subq_14.visit__user
                        , subq_14.visit__session
                        , subq_14.referrer_id
                        , subq_14.visit__referrer_id
                        , subq_14.visits
                        , subq_14.visitors
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
                          , CASE WHEN EXTRACT(dow FROM visits_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM visits_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM visits_source_src_28000.ds) END AS ds__extract_dow
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
                          , CASE WHEN EXTRACT(dow FROM visits_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM visits_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM visits_source_src_28000.ds) END AS visit__ds__extract_dow
                          , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                          , visits_source_src_28000.referrer_id AS visit__referrer_id
                          , visits_source_src_28000.user_id AS user
                          , visits_source_src_28000.session_id AS session
                          , visits_source_src_28000.user_id AS visit__user
                          , visits_source_src_28000.session_id AS visit__session
                        FROM ***************************.fct_visits visits_source_src_28000
                      ) subq_14
                    ) subq_15
                  ) subq_16
                  GROUP BY
                    subq_16.user
                ) subq_17
                FULL OUTER JOIN (
                  -- Aggregate Measures
                  SELECT
                    subq_25.user
                    , SUM(subq_25.buys) AS buys
                  FROM (
                    -- Pass Only Elements: ['buys', 'user']
                    SELECT
                      subq_24.user
                      , subq_24.buys
                    FROM (
                      -- Find conversions for user within the range of INF
                      SELECT
                        subq_23.metric_time__day
                        , subq_23.user
                        , subq_23.buys
                        , subq_23.visits
                      FROM (
                        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
                        SELECT DISTINCT
                          FIRST_VALUE(subq_19.visits) OVER (
                            PARTITION BY
                              subq_22.user
                              , subq_22.metric_time__day
                              , subq_22.mf_internal_uuid
                            ORDER BY subq_19.metric_time__day DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                          ) AS visits
                          , FIRST_VALUE(subq_19.metric_time__day) OVER (
                            PARTITION BY
                              subq_22.user
                              , subq_22.metric_time__day
                              , subq_22.mf_internal_uuid
                            ORDER BY subq_19.metric_time__day DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                          ) AS metric_time__day
                          , FIRST_VALUE(subq_19.user) OVER (
                            PARTITION BY
                              subq_22.user
                              , subq_22.metric_time__day
                              , subq_22.mf_internal_uuid
                            ORDER BY subq_19.metric_time__day DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                          ) AS user
                          , subq_22.mf_internal_uuid AS mf_internal_uuid
                          , subq_22.buys AS buys
                        FROM (
                          -- Pass Only Elements: ['visits', 'metric_time__day', 'user']
                          SELECT
                            subq_18.metric_time__day
                            , subq_18.user
                            , subq_18.visits
                          FROM (
                            -- Metric Time Dimension 'ds'
                            SELECT
                              subq_14.ds__day
                              , subq_14.ds__week
                              , subq_14.ds__month
                              , subq_14.ds__quarter
                              , subq_14.ds__year
                              , subq_14.ds__extract_year
                              , subq_14.ds__extract_quarter
                              , subq_14.ds__extract_month
                              , subq_14.ds__extract_day
                              , subq_14.ds__extract_dow
                              , subq_14.ds__extract_doy
                              , subq_14.visit__ds__day
                              , subq_14.visit__ds__week
                              , subq_14.visit__ds__month
                              , subq_14.visit__ds__quarter
                              , subq_14.visit__ds__year
                              , subq_14.visit__ds__extract_year
                              , subq_14.visit__ds__extract_quarter
                              , subq_14.visit__ds__extract_month
                              , subq_14.visit__ds__extract_day
                              , subq_14.visit__ds__extract_dow
                              , subq_14.visit__ds__extract_doy
                              , subq_14.ds__day AS metric_time__day
                              , subq_14.ds__week AS metric_time__week
                              , subq_14.ds__month AS metric_time__month
                              , subq_14.ds__quarter AS metric_time__quarter
                              , subq_14.ds__year AS metric_time__year
                              , subq_14.ds__extract_year AS metric_time__extract_year
                              , subq_14.ds__extract_quarter AS metric_time__extract_quarter
                              , subq_14.ds__extract_month AS metric_time__extract_month
                              , subq_14.ds__extract_day AS metric_time__extract_day
                              , subq_14.ds__extract_dow AS metric_time__extract_dow
                              , subq_14.ds__extract_doy AS metric_time__extract_doy
                              , subq_14.user
                              , subq_14.session
                              , subq_14.visit__user
                              , subq_14.visit__session
                              , subq_14.referrer_id
                              , subq_14.visit__referrer_id
                              , subq_14.visits
                              , subq_14.visitors
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
                                , CASE WHEN EXTRACT(dow FROM visits_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM visits_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM visits_source_src_28000.ds) END AS ds__extract_dow
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
                                , CASE WHEN EXTRACT(dow FROM visits_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM visits_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM visits_source_src_28000.ds) END AS visit__ds__extract_dow
                                , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                                , visits_source_src_28000.referrer_id AS visit__referrer_id
                                , visits_source_src_28000.user_id AS user
                                , visits_source_src_28000.session_id AS session
                                , visits_source_src_28000.user_id AS visit__user
                                , visits_source_src_28000.session_id AS visit__session
                              FROM ***************************.fct_visits visits_source_src_28000
                            ) subq_14
                          ) subq_18
                        ) subq_19
                        INNER JOIN (
                          -- Add column with generated UUID
                          SELECT
                            subq_21.ds__day
                            , subq_21.ds__week
                            , subq_21.ds__month
                            , subq_21.ds__quarter
                            , subq_21.ds__year
                            , subq_21.ds__extract_year
                            , subq_21.ds__extract_quarter
                            , subq_21.ds__extract_month
                            , subq_21.ds__extract_day
                            , subq_21.ds__extract_dow
                            , subq_21.ds__extract_doy
                            , subq_21.ds_month__month
                            , subq_21.ds_month__quarter
                            , subq_21.ds_month__year
                            , subq_21.ds_month__extract_year
                            , subq_21.ds_month__extract_quarter
                            , subq_21.ds_month__extract_month
                            , subq_21.buy__ds__day
                            , subq_21.buy__ds__week
                            , subq_21.buy__ds__month
                            , subq_21.buy__ds__quarter
                            , subq_21.buy__ds__year
                            , subq_21.buy__ds__extract_year
                            , subq_21.buy__ds__extract_quarter
                            , subq_21.buy__ds__extract_month
                            , subq_21.buy__ds__extract_day
                            , subq_21.buy__ds__extract_dow
                            , subq_21.buy__ds__extract_doy
                            , subq_21.buy__ds_month__month
                            , subq_21.buy__ds_month__quarter
                            , subq_21.buy__ds_month__year
                            , subq_21.buy__ds_month__extract_year
                            , subq_21.buy__ds_month__extract_quarter
                            , subq_21.buy__ds_month__extract_month
                            , subq_21.metric_time__day
                            , subq_21.metric_time__week
                            , subq_21.metric_time__month
                            , subq_21.metric_time__quarter
                            , subq_21.metric_time__year
                            , subq_21.metric_time__extract_year
                            , subq_21.metric_time__extract_quarter
                            , subq_21.metric_time__extract_month
                            , subq_21.metric_time__extract_day
                            , subq_21.metric_time__extract_dow
                            , subq_21.metric_time__extract_doy
                            , subq_21.user
                            , subq_21.session_id
                            , subq_21.buy__user
                            , subq_21.buy__session_id
                            , subq_21.buys
                            , subq_21.buyers
                            , CONCAT(CAST(RANDOM()*100000000 AS INT)::VARCHAR,CAST(RANDOM()*100000000 AS INT)::VARCHAR) AS mf_internal_uuid
                          FROM (
                            -- Metric Time Dimension 'ds'
                            SELECT
                              subq_20.ds__day
                              , subq_20.ds__week
                              , subq_20.ds__month
                              , subq_20.ds__quarter
                              , subq_20.ds__year
                              , subq_20.ds__extract_year
                              , subq_20.ds__extract_quarter
                              , subq_20.ds__extract_month
                              , subq_20.ds__extract_day
                              , subq_20.ds__extract_dow
                              , subq_20.ds__extract_doy
                              , subq_20.ds_month__month
                              , subq_20.ds_month__quarter
                              , subq_20.ds_month__year
                              , subq_20.ds_month__extract_year
                              , subq_20.ds_month__extract_quarter
                              , subq_20.ds_month__extract_month
                              , subq_20.buy__ds__day
                              , subq_20.buy__ds__week
                              , subq_20.buy__ds__month
                              , subq_20.buy__ds__quarter
                              , subq_20.buy__ds__year
                              , subq_20.buy__ds__extract_year
                              , subq_20.buy__ds__extract_quarter
                              , subq_20.buy__ds__extract_month
                              , subq_20.buy__ds__extract_day
                              , subq_20.buy__ds__extract_dow
                              , subq_20.buy__ds__extract_doy
                              , subq_20.buy__ds_month__month
                              , subq_20.buy__ds_month__quarter
                              , subq_20.buy__ds_month__year
                              , subq_20.buy__ds_month__extract_year
                              , subq_20.buy__ds_month__extract_quarter
                              , subq_20.buy__ds_month__extract_month
                              , subq_20.ds__day AS metric_time__day
                              , subq_20.ds__week AS metric_time__week
                              , subq_20.ds__month AS metric_time__month
                              , subq_20.ds__quarter AS metric_time__quarter
                              , subq_20.ds__year AS metric_time__year
                              , subq_20.ds__extract_year AS metric_time__extract_year
                              , subq_20.ds__extract_quarter AS metric_time__extract_quarter
                              , subq_20.ds__extract_month AS metric_time__extract_month
                              , subq_20.ds__extract_day AS metric_time__extract_day
                              , subq_20.ds__extract_dow AS metric_time__extract_dow
                              , subq_20.ds__extract_doy AS metric_time__extract_doy
                              , subq_20.user
                              , subq_20.session_id
                              , subq_20.buy__user
                              , subq_20.buy__session_id
                              , subq_20.buys
                              , subq_20.buyers
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
                                , CASE WHEN EXTRACT(dow FROM buys_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM buys_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM buys_source_src_28000.ds) END AS ds__extract_dow
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
                                , CASE WHEN EXTRACT(dow FROM buys_source_src_28000.ds) = 0 THEN EXTRACT(dow FROM buys_source_src_28000.ds) + 7 ELSE EXTRACT(dow FROM buys_source_src_28000.ds) END AS buy__ds__extract_dow
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
                            ) subq_20
                          ) subq_21
                        ) subq_22
                        ON
                          (
                            subq_19.user = subq_22.user
                          ) AND (
                            (subq_19.metric_time__day <= subq_22.metric_time__day)
                          )
                      ) subq_23
                    ) subq_24
                  ) subq_25
                  GROUP BY
                    subq_25.user
                ) subq_26
                ON
                  subq_17.user = subq_26.user
                GROUP BY
                  COALESCE(subq_17.user, subq_26.user)
              ) subq_27
            ) subq_28
          ) subq_29
          ON
            subq_13.user = subq_29.user
        ) subq_30
        WHERE user__visit_buy_conversion_rate > 2
      ) subq_31
    ) subq_32
  ) subq_33
) subq_34
