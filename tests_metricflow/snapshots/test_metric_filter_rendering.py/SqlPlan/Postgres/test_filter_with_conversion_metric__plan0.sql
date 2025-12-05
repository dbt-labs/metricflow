test_name: test_filter_with_conversion_metric
test_filename: test_metric_filter_rendering.py
sql_engine: Postgres
---
-- Write to DataTable
SELECT
  subq_41.listings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_40.__listings AS listings
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      SUM(subq_39.__listings) AS __listings
    FROM (
      -- Pass Only Elements: ['__listings']
      SELECT
        subq_38.__listings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_37.listings AS __listings
          , subq_37.user__visit_buy_conversion_rate
        FROM (
          -- Pass Only Elements: ['__listings', 'user__visit_buy_conversion_rate']
          SELECT
            subq_36.user__visit_buy_conversion_rate
            , subq_36.__listings AS listings
          FROM (
            -- Join Standard Outputs
            SELECT
              subq_35.user__visit_buy_conversion_rate AS user__visit_buy_conversion_rate
              , subq_16.ds__day AS ds__day
              , subq_16.ds__week AS ds__week
              , subq_16.ds__month AS ds__month
              , subq_16.ds__quarter AS ds__quarter
              , subq_16.ds__year AS ds__year
              , subq_16.ds__extract_year AS ds__extract_year
              , subq_16.ds__extract_quarter AS ds__extract_quarter
              , subq_16.ds__extract_month AS ds__extract_month
              , subq_16.ds__extract_day AS ds__extract_day
              , subq_16.ds__extract_dow AS ds__extract_dow
              , subq_16.ds__extract_doy AS ds__extract_doy
              , subq_16.created_at__day AS created_at__day
              , subq_16.created_at__week AS created_at__week
              , subq_16.created_at__month AS created_at__month
              , subq_16.created_at__quarter AS created_at__quarter
              , subq_16.created_at__year AS created_at__year
              , subq_16.created_at__extract_year AS created_at__extract_year
              , subq_16.created_at__extract_quarter AS created_at__extract_quarter
              , subq_16.created_at__extract_month AS created_at__extract_month
              , subq_16.created_at__extract_day AS created_at__extract_day
              , subq_16.created_at__extract_dow AS created_at__extract_dow
              , subq_16.created_at__extract_doy AS created_at__extract_doy
              , subq_16.listing__ds__day AS listing__ds__day
              , subq_16.listing__ds__week AS listing__ds__week
              , subq_16.listing__ds__month AS listing__ds__month
              , subq_16.listing__ds__quarter AS listing__ds__quarter
              , subq_16.listing__ds__year AS listing__ds__year
              , subq_16.listing__ds__extract_year AS listing__ds__extract_year
              , subq_16.listing__ds__extract_quarter AS listing__ds__extract_quarter
              , subq_16.listing__ds__extract_month AS listing__ds__extract_month
              , subq_16.listing__ds__extract_day AS listing__ds__extract_day
              , subq_16.listing__ds__extract_dow AS listing__ds__extract_dow
              , subq_16.listing__ds__extract_doy AS listing__ds__extract_doy
              , subq_16.listing__created_at__day AS listing__created_at__day
              , subq_16.listing__created_at__week AS listing__created_at__week
              , subq_16.listing__created_at__month AS listing__created_at__month
              , subq_16.listing__created_at__quarter AS listing__created_at__quarter
              , subq_16.listing__created_at__year AS listing__created_at__year
              , subq_16.listing__created_at__extract_year AS listing__created_at__extract_year
              , subq_16.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
              , subq_16.listing__created_at__extract_month AS listing__created_at__extract_month
              , subq_16.listing__created_at__extract_day AS listing__created_at__extract_day
              , subq_16.listing__created_at__extract_dow AS listing__created_at__extract_dow
              , subq_16.listing__created_at__extract_doy AS listing__created_at__extract_doy
              , subq_16.metric_time__day AS metric_time__day
              , subq_16.metric_time__week AS metric_time__week
              , subq_16.metric_time__month AS metric_time__month
              , subq_16.metric_time__quarter AS metric_time__quarter
              , subq_16.metric_time__year AS metric_time__year
              , subq_16.metric_time__extract_year AS metric_time__extract_year
              , subq_16.metric_time__extract_quarter AS metric_time__extract_quarter
              , subq_16.metric_time__extract_month AS metric_time__extract_month
              , subq_16.metric_time__extract_day AS metric_time__extract_day
              , subq_16.metric_time__extract_dow AS metric_time__extract_dow
              , subq_16.metric_time__extract_doy AS metric_time__extract_doy
              , subq_16.listing AS listing
              , subq_16.user AS user
              , subq_16.listing__user AS listing__user
              , subq_16.country_latest AS country_latest
              , subq_16.is_lux_latest AS is_lux_latest
              , subq_16.capacity_latest AS capacity_latest
              , subq_16.listing__country_latest AS listing__country_latest
              , subq_16.listing__is_lux_latest AS listing__is_lux_latest
              , subq_16.listing__capacity_latest AS listing__capacity_latest
              , subq_16.__listings AS __listings
              , subq_16.__lux_listings AS __lux_listings
              , subq_16.__smallest_listing AS __smallest_listing
              , subq_16.__largest_listing AS __largest_listing
              , subq_16.__active_listings AS __active_listings
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_15.ds__day
                , subq_15.ds__week
                , subq_15.ds__month
                , subq_15.ds__quarter
                , subq_15.ds__year
                , subq_15.ds__extract_year
                , subq_15.ds__extract_quarter
                , subq_15.ds__extract_month
                , subq_15.ds__extract_day
                , subq_15.ds__extract_dow
                , subq_15.ds__extract_doy
                , subq_15.created_at__day
                , subq_15.created_at__week
                , subq_15.created_at__month
                , subq_15.created_at__quarter
                , subq_15.created_at__year
                , subq_15.created_at__extract_year
                , subq_15.created_at__extract_quarter
                , subq_15.created_at__extract_month
                , subq_15.created_at__extract_day
                , subq_15.created_at__extract_dow
                , subq_15.created_at__extract_doy
                , subq_15.listing__ds__day
                , subq_15.listing__ds__week
                , subq_15.listing__ds__month
                , subq_15.listing__ds__quarter
                , subq_15.listing__ds__year
                , subq_15.listing__ds__extract_year
                , subq_15.listing__ds__extract_quarter
                , subq_15.listing__ds__extract_month
                , subq_15.listing__ds__extract_day
                , subq_15.listing__ds__extract_dow
                , subq_15.listing__ds__extract_doy
                , subq_15.listing__created_at__day
                , subq_15.listing__created_at__week
                , subq_15.listing__created_at__month
                , subq_15.listing__created_at__quarter
                , subq_15.listing__created_at__year
                , subq_15.listing__created_at__extract_year
                , subq_15.listing__created_at__extract_quarter
                , subq_15.listing__created_at__extract_month
                , subq_15.listing__created_at__extract_day
                , subq_15.listing__created_at__extract_dow
                , subq_15.listing__created_at__extract_doy
                , subq_15.ds__day AS metric_time__day
                , subq_15.ds__week AS metric_time__week
                , subq_15.ds__month AS metric_time__month
                , subq_15.ds__quarter AS metric_time__quarter
                , subq_15.ds__year AS metric_time__year
                , subq_15.ds__extract_year AS metric_time__extract_year
                , subq_15.ds__extract_quarter AS metric_time__extract_quarter
                , subq_15.ds__extract_month AS metric_time__extract_month
                , subq_15.ds__extract_day AS metric_time__extract_day
                , subq_15.ds__extract_dow AS metric_time__extract_dow
                , subq_15.ds__extract_doy AS metric_time__extract_doy
                , subq_15.listing
                , subq_15.user
                , subq_15.listing__user
                , subq_15.country_latest
                , subq_15.is_lux_latest
                , subq_15.capacity_latest
                , subq_15.listing__country_latest
                , subq_15.listing__is_lux_latest
                , subq_15.listing__capacity_latest
                , subq_15.__listings
                , subq_15.__lux_listings
                , subq_15.__smallest_listing
                , subq_15.__largest_listing
                , subq_15.__active_listings
              FROM (
                -- Read Elements From Semantic Model 'listings_latest'
                SELECT
                  1 AS __listings
                  , 1 AS __lux_listings
                  , listings_latest_src_28000.capacity AS __smallest_listing
                  , listings_latest_src_28000.capacity AS __largest_listing
                  , 1 AS __active_listings
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
              ) subq_15
            ) subq_16
            LEFT OUTER JOIN (
              -- Pass Only Elements: ['user', 'user__visit_buy_conversion_rate']
              SELECT
                subq_34.user
                , subq_34.user__visit_buy_conversion_rate
              FROM (
                -- Compute Metrics via Expressions
                SELECT
                  subq_33.user
                  , CAST(subq_33.__buys AS DOUBLE PRECISION) / CAST(NULLIF(subq_33.__visits, 0) AS DOUBLE PRECISION) AS user__visit_buy_conversion_rate
                FROM (
                  -- Combine Aggregated Outputs
                  SELECT
                    COALESCE(subq_21.user, subq_32.user) AS user
                    , MAX(subq_21.__visits) AS __visits
                    , MAX(subq_32.__buys) AS __buys
                  FROM (
                    -- Aggregate Inputs for Simple Metrics
                    SELECT
                      subq_20.user
                      , SUM(subq_20.__visits) AS __visits
                    FROM (
                      -- Pass Only Elements: ['__visits', 'user']
                      SELECT
                        subq_19.user
                        , subq_19.__visits
                      FROM (
                        -- Pass Only Elements: ['__visits', 'user']
                        SELECT
                          subq_18.user
                          , subq_18.__visits
                        FROM (
                          -- Metric Time Dimension 'ds'
                          SELECT
                            subq_17.ds__day
                            , subq_17.ds__week
                            , subq_17.ds__month
                            , subq_17.ds__quarter
                            , subq_17.ds__year
                            , subq_17.ds__extract_year
                            , subq_17.ds__extract_quarter
                            , subq_17.ds__extract_month
                            , subq_17.ds__extract_day
                            , subq_17.ds__extract_dow
                            , subq_17.ds__extract_doy
                            , subq_17.visit__ds__day
                            , subq_17.visit__ds__week
                            , subq_17.visit__ds__month
                            , subq_17.visit__ds__quarter
                            , subq_17.visit__ds__year
                            , subq_17.visit__ds__extract_year
                            , subq_17.visit__ds__extract_quarter
                            , subq_17.visit__ds__extract_month
                            , subq_17.visit__ds__extract_day
                            , subq_17.visit__ds__extract_dow
                            , subq_17.visit__ds__extract_doy
                            , subq_17.ds__day AS metric_time__day
                            , subq_17.ds__week AS metric_time__week
                            , subq_17.ds__month AS metric_time__month
                            , subq_17.ds__quarter AS metric_time__quarter
                            , subq_17.ds__year AS metric_time__year
                            , subq_17.ds__extract_year AS metric_time__extract_year
                            , subq_17.ds__extract_quarter AS metric_time__extract_quarter
                            , subq_17.ds__extract_month AS metric_time__extract_month
                            , subq_17.ds__extract_day AS metric_time__extract_day
                            , subq_17.ds__extract_dow AS metric_time__extract_dow
                            , subq_17.ds__extract_doy AS metric_time__extract_doy
                            , subq_17.user
                            , subq_17.session
                            , subq_17.visit__user
                            , subq_17.visit__session
                            , subq_17.referrer_id
                            , subq_17.visit__referrer_id
                            , subq_17.__visits
                            , subq_17.__visits_fill_nulls_with_0_join_to_timespine
                          FROM (
                            -- Read Elements From Semantic Model 'visits_source'
                            SELECT
                              1 AS __visits
                              , 1 AS __visits_fill_nulls_with_0_join_to_timespine
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
                          ) subq_17
                        ) subq_18
                      ) subq_19
                    ) subq_20
                    GROUP BY
                      subq_20.user
                  ) subq_21
                  FULL OUTER JOIN (
                    -- Aggregate Inputs for Simple Metrics
                    SELECT
                      subq_31.user
                      , SUM(subq_31.__buys) AS __buys
                    FROM (
                      -- Pass Only Elements: ['__buys', 'user']
                      SELECT
                        subq_30.user
                        , subq_30.__buys
                      FROM (
                        -- Pass Only Elements: ['__buys', 'user']
                        SELECT
                          subq_29.user
                          , subq_29.__buys
                        FROM (
                          -- Find conversions for user within the range of INF
                          SELECT
                            subq_28.metric_time__day
                            , subq_28.user
                            , subq_28.__buys
                            , subq_28.__visits
                          FROM (
                            -- Dedupe the fanout with mf_internal_uuid in the conversion data set
                            SELECT DISTINCT
                              FIRST_VALUE(subq_24.__visits) OVER (
                                PARTITION BY
                                  subq_27.user
                                  , subq_27.metric_time__day
                                  , subq_27.mf_internal_uuid
                                ORDER BY subq_24.metric_time__day DESC
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                              ) AS __visits
                              , FIRST_VALUE(subq_24.metric_time__day) OVER (
                                PARTITION BY
                                  subq_27.user
                                  , subq_27.metric_time__day
                                  , subq_27.mf_internal_uuid
                                ORDER BY subq_24.metric_time__day DESC
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                              ) AS metric_time__day
                              , FIRST_VALUE(subq_24.user) OVER (
                                PARTITION BY
                                  subq_27.user
                                  , subq_27.metric_time__day
                                  , subq_27.mf_internal_uuid
                                ORDER BY subq_24.metric_time__day DESC
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                              ) AS user
                              , subq_27.mf_internal_uuid AS mf_internal_uuid
                              , subq_27.__buys AS __buys
                            FROM (
                              -- Pass Only Elements: ['__visits', 'metric_time__day', 'user']
                              SELECT
                                subq_23.metric_time__day
                                , subq_23.user
                                , subq_23.__visits
                              FROM (
                                -- Pass Only Elements: ['__visits', 'metric_time__day', 'user']
                                SELECT
                                  subq_22.metric_time__day
                                  , subq_22.user
                                  , subq_22.__visits
                                FROM (
                                  -- Metric Time Dimension 'ds'
                                  SELECT
                                    subq_17.ds__day
                                    , subq_17.ds__week
                                    , subq_17.ds__month
                                    , subq_17.ds__quarter
                                    , subq_17.ds__year
                                    , subq_17.ds__extract_year
                                    , subq_17.ds__extract_quarter
                                    , subq_17.ds__extract_month
                                    , subq_17.ds__extract_day
                                    , subq_17.ds__extract_dow
                                    , subq_17.ds__extract_doy
                                    , subq_17.visit__ds__day
                                    , subq_17.visit__ds__week
                                    , subq_17.visit__ds__month
                                    , subq_17.visit__ds__quarter
                                    , subq_17.visit__ds__year
                                    , subq_17.visit__ds__extract_year
                                    , subq_17.visit__ds__extract_quarter
                                    , subq_17.visit__ds__extract_month
                                    , subq_17.visit__ds__extract_day
                                    , subq_17.visit__ds__extract_dow
                                    , subq_17.visit__ds__extract_doy
                                    , subq_17.ds__day AS metric_time__day
                                    , subq_17.ds__week AS metric_time__week
                                    , subq_17.ds__month AS metric_time__month
                                    , subq_17.ds__quarter AS metric_time__quarter
                                    , subq_17.ds__year AS metric_time__year
                                    , subq_17.ds__extract_year AS metric_time__extract_year
                                    , subq_17.ds__extract_quarter AS metric_time__extract_quarter
                                    , subq_17.ds__extract_month AS metric_time__extract_month
                                    , subq_17.ds__extract_day AS metric_time__extract_day
                                    , subq_17.ds__extract_dow AS metric_time__extract_dow
                                    , subq_17.ds__extract_doy AS metric_time__extract_doy
                                    , subq_17.user
                                    , subq_17.session
                                    , subq_17.visit__user
                                    , subq_17.visit__session
                                    , subq_17.referrer_id
                                    , subq_17.visit__referrer_id
                                    , subq_17.__visits
                                    , subq_17.__visits_fill_nulls_with_0_join_to_timespine
                                  FROM (
                                    -- Read Elements From Semantic Model 'visits_source'
                                    SELECT
                                      1 AS __visits
                                      , 1 AS __visits_fill_nulls_with_0_join_to_timespine
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
                                  ) subq_17
                                ) subq_22
                              ) subq_23
                            ) subq_24
                            INNER JOIN (
                              -- Add column with generated UUID
                              SELECT
                                subq_26.ds__day
                                , subq_26.ds__week
                                , subq_26.ds__month
                                , subq_26.ds__quarter
                                , subq_26.ds__year
                                , subq_26.ds__extract_year
                                , subq_26.ds__extract_quarter
                                , subq_26.ds__extract_month
                                , subq_26.ds__extract_day
                                , subq_26.ds__extract_dow
                                , subq_26.ds__extract_doy
                                , subq_26.ds_month__month
                                , subq_26.ds_month__quarter
                                , subq_26.ds_month__year
                                , subq_26.ds_month__extract_year
                                , subq_26.ds_month__extract_quarter
                                , subq_26.ds_month__extract_month
                                , subq_26.buy__ds__day
                                , subq_26.buy__ds__week
                                , subq_26.buy__ds__month
                                , subq_26.buy__ds__quarter
                                , subq_26.buy__ds__year
                                , subq_26.buy__ds__extract_year
                                , subq_26.buy__ds__extract_quarter
                                , subq_26.buy__ds__extract_month
                                , subq_26.buy__ds__extract_day
                                , subq_26.buy__ds__extract_dow
                                , subq_26.buy__ds__extract_doy
                                , subq_26.buy__ds_month__month
                                , subq_26.buy__ds_month__quarter
                                , subq_26.buy__ds_month__year
                                , subq_26.buy__ds_month__extract_year
                                , subq_26.buy__ds_month__extract_quarter
                                , subq_26.buy__ds_month__extract_month
                                , subq_26.metric_time__day
                                , subq_26.metric_time__week
                                , subq_26.metric_time__month
                                , subq_26.metric_time__quarter
                                , subq_26.metric_time__year
                                , subq_26.metric_time__extract_year
                                , subq_26.metric_time__extract_quarter
                                , subq_26.metric_time__extract_month
                                , subq_26.metric_time__extract_day
                                , subq_26.metric_time__extract_dow
                                , subq_26.metric_time__extract_doy
                                , subq_26.user
                                , subq_26.session_id
                                , subq_26.buy__user
                                , subq_26.buy__session_id
                                , subq_26.__buys
                                , subq_26.__buys_fill_nulls_with_0
                                , subq_26.__buys_fill_nulls_with_0_join_to_timespine
                                , GEN_RANDOM_UUID() AS mf_internal_uuid
                              FROM (
                                -- Metric Time Dimension 'ds'
                                SELECT
                                  subq_25.ds__day
                                  , subq_25.ds__week
                                  , subq_25.ds__month
                                  , subq_25.ds__quarter
                                  , subq_25.ds__year
                                  , subq_25.ds__extract_year
                                  , subq_25.ds__extract_quarter
                                  , subq_25.ds__extract_month
                                  , subq_25.ds__extract_day
                                  , subq_25.ds__extract_dow
                                  , subq_25.ds__extract_doy
                                  , subq_25.ds_month__month
                                  , subq_25.ds_month__quarter
                                  , subq_25.ds_month__year
                                  , subq_25.ds_month__extract_year
                                  , subq_25.ds_month__extract_quarter
                                  , subq_25.ds_month__extract_month
                                  , subq_25.buy__ds__day
                                  , subq_25.buy__ds__week
                                  , subq_25.buy__ds__month
                                  , subq_25.buy__ds__quarter
                                  , subq_25.buy__ds__year
                                  , subq_25.buy__ds__extract_year
                                  , subq_25.buy__ds__extract_quarter
                                  , subq_25.buy__ds__extract_month
                                  , subq_25.buy__ds__extract_day
                                  , subq_25.buy__ds__extract_dow
                                  , subq_25.buy__ds__extract_doy
                                  , subq_25.buy__ds_month__month
                                  , subq_25.buy__ds_month__quarter
                                  , subq_25.buy__ds_month__year
                                  , subq_25.buy__ds_month__extract_year
                                  , subq_25.buy__ds_month__extract_quarter
                                  , subq_25.buy__ds_month__extract_month
                                  , subq_25.ds__day AS metric_time__day
                                  , subq_25.ds__week AS metric_time__week
                                  , subq_25.ds__month AS metric_time__month
                                  , subq_25.ds__quarter AS metric_time__quarter
                                  , subq_25.ds__year AS metric_time__year
                                  , subq_25.ds__extract_year AS metric_time__extract_year
                                  , subq_25.ds__extract_quarter AS metric_time__extract_quarter
                                  , subq_25.ds__extract_month AS metric_time__extract_month
                                  , subq_25.ds__extract_day AS metric_time__extract_day
                                  , subq_25.ds__extract_dow AS metric_time__extract_dow
                                  , subq_25.ds__extract_doy AS metric_time__extract_doy
                                  , subq_25.user
                                  , subq_25.session_id
                                  , subq_25.buy__user
                                  , subq_25.buy__session_id
                                  , subq_25.__buys
                                  , subq_25.__buys_fill_nulls_with_0
                                  , subq_25.__buys_fill_nulls_with_0_join_to_timespine
                                FROM (
                                  -- Read Elements From Semantic Model 'buys_source'
                                  SELECT
                                    1 AS __buys
                                    , 1 AS __buys_fill_nulls_with_0
                                    , 1 AS __buys_fill_nulls_with_0_join_to_timespine
                                    , 1 AS __buys_month
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
                                ) subq_25
                              ) subq_26
                            ) subq_27
                            ON
                              (
                                subq_24.user = subq_27.user
                              ) AND (
                                (subq_24.metric_time__day <= subq_27.metric_time__day)
                              )
                          ) subq_28
                        ) subq_29
                      ) subq_30
                    ) subq_31
                    GROUP BY
                      subq_31.user
                  ) subq_32
                  ON
                    subq_21.user = subq_32.user
                  GROUP BY
                    COALESCE(subq_21.user, subq_32.user)
                ) subq_33
              ) subq_34
            ) subq_35
            ON
              subq_16.user = subq_35.user
          ) subq_36
        ) subq_37
        WHERE user__visit_buy_conversion_rate > 2
      ) subq_38
    ) subq_39
  ) subq_40
) subq_41
