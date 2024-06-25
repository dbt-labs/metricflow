-- Compute Metrics via Expressions
SELECT
  subq_24.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_23.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_22.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_21.user__visit_buy_conversion_rate
        , subq_21.listings
      FROM (
        -- Pass Only Elements: ['listings', 'user__visit_buy_conversion_rate']
        SELECT
          subq_20.user__visit_buy_conversion_rate
          , subq_20.listings
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_2.user AS user
            , subq_19.user__visit_buy_conversion_rate AS user__visit_buy_conversion_rate
            , subq_2.listings AS listings
          FROM (
            -- Pass Only Elements: ['listings', 'user']
            SELECT
              subq_1.user
              , subq_1.listings
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
          ) subq_2
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['user', 'user__visit_buy_conversion_rate']
            SELECT
              subq_18.user
              , subq_18.user__visit_buy_conversion_rate
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_17.user
                , CAST(subq_17.buys AS DOUBLE) / CAST(NULLIF(subq_17.visits, 0) AS DOUBLE) AS user__visit_buy_conversion_rate
              FROM (
                -- Combine Aggregated Outputs
                SELECT
                  COALESCE(subq_6.user, subq_16.user) AS user
                  , MAX(subq_6.visits) AS visits
                  , MAX(subq_16.buys) AS buys
                FROM (
                  -- Aggregate Measures
                  SELECT
                    subq_5.user
                    , SUM(subq_5.visits) AS visits
                  FROM (
                    -- Pass Only Elements: ['visits', 'user']
                    SELECT
                      subq_4.user
                      , subq_4.visits
                    FROM (
                      -- Metric Time Dimension 'ds'
                      SELECT
                        subq_3.ds__day
                        , subq_3.ds__week
                        , subq_3.ds__month
                        , subq_3.ds__quarter
                        , subq_3.ds__year
                        , subq_3.ds__extract_year
                        , subq_3.ds__extract_quarter
                        , subq_3.ds__extract_month
                        , subq_3.ds__extract_day
                        , subq_3.ds__extract_dow
                        , subq_3.ds__extract_doy
                        , subq_3.visit__ds__day
                        , subq_3.visit__ds__week
                        , subq_3.visit__ds__month
                        , subq_3.visit__ds__quarter
                        , subq_3.visit__ds__year
                        , subq_3.visit__ds__extract_year
                        , subq_3.visit__ds__extract_quarter
                        , subq_3.visit__ds__extract_month
                        , subq_3.visit__ds__extract_day
                        , subq_3.visit__ds__extract_dow
                        , subq_3.visit__ds__extract_doy
                        , subq_3.ds__day AS metric_time__day
                        , subq_3.ds__week AS metric_time__week
                        , subq_3.ds__month AS metric_time__month
                        , subq_3.ds__quarter AS metric_time__quarter
                        , subq_3.ds__year AS metric_time__year
                        , subq_3.ds__extract_year AS metric_time__extract_year
                        , subq_3.ds__extract_quarter AS metric_time__extract_quarter
                        , subq_3.ds__extract_month AS metric_time__extract_month
                        , subq_3.ds__extract_day AS metric_time__extract_day
                        , subq_3.ds__extract_dow AS metric_time__extract_dow
                        , subq_3.ds__extract_doy AS metric_time__extract_doy
                        , subq_3.user
                        , subq_3.session
                        , subq_3.visit__user
                        , subq_3.visit__session
                        , subq_3.referrer_id
                        , subq_3.visit__referrer_id
                        , subq_3.visits
                        , subq_3.visitors
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
                      ) subq_3
                    ) subq_4
                  ) subq_5
                  GROUP BY
                    subq_5.user
                ) subq_6
                FULL OUTER JOIN (
                  -- Aggregate Measures
                  SELECT
                    subq_15.user
                    , SUM(subq_15.buys) AS buys
                  FROM (
                    -- Pass Only Elements: ['buys', 'user']
                    SELECT
                      subq_14.user
                      , subq_14.buys
                    FROM (
                      -- Find conversions for user within the range of INF
                      SELECT
                        subq_13.ds__day
                        , subq_13.user
                        , subq_13.buys
                        , subq_13.visits
                      FROM (
                        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
                        SELECT DISTINCT
                          FIRST_VALUE(subq_9.visits) OVER (
                            PARTITION BY
                              subq_12.user
                              , subq_12.ds__day
                              , subq_12.mf_internal_uuid
                            ORDER BY subq_9.ds__day DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                          ) AS visits
                          , FIRST_VALUE(subq_9.ds__day) OVER (
                            PARTITION BY
                              subq_12.user
                              , subq_12.ds__day
                              , subq_12.mf_internal_uuid
                            ORDER BY subq_9.ds__day DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                          ) AS ds__day
                          , FIRST_VALUE(subq_9.user) OVER (
                            PARTITION BY
                              subq_12.user
                              , subq_12.ds__day
                              , subq_12.mf_internal_uuid
                            ORDER BY subq_9.ds__day DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                          ) AS user
                          , subq_12.mf_internal_uuid AS mf_internal_uuid
                          , subq_12.buys AS buys
                        FROM (
                          -- Pass Only Elements: ['visits', 'ds__day', 'user']
                          SELECT
                            subq_8.ds__day
                            , subq_8.user
                            , subq_8.visits
                          FROM (
                            -- Metric Time Dimension 'ds'
                            SELECT
                              subq_7.ds__day
                              , subq_7.ds__week
                              , subq_7.ds__month
                              , subq_7.ds__quarter
                              , subq_7.ds__year
                              , subq_7.ds__extract_year
                              , subq_7.ds__extract_quarter
                              , subq_7.ds__extract_month
                              , subq_7.ds__extract_day
                              , subq_7.ds__extract_dow
                              , subq_7.ds__extract_doy
                              , subq_7.visit__ds__day
                              , subq_7.visit__ds__week
                              , subq_7.visit__ds__month
                              , subq_7.visit__ds__quarter
                              , subq_7.visit__ds__year
                              , subq_7.visit__ds__extract_year
                              , subq_7.visit__ds__extract_quarter
                              , subq_7.visit__ds__extract_month
                              , subq_7.visit__ds__extract_day
                              , subq_7.visit__ds__extract_dow
                              , subq_7.visit__ds__extract_doy
                              , subq_7.ds__day AS metric_time__day
                              , subq_7.ds__week AS metric_time__week
                              , subq_7.ds__month AS metric_time__month
                              , subq_7.ds__quarter AS metric_time__quarter
                              , subq_7.ds__year AS metric_time__year
                              , subq_7.ds__extract_year AS metric_time__extract_year
                              , subq_7.ds__extract_quarter AS metric_time__extract_quarter
                              , subq_7.ds__extract_month AS metric_time__extract_month
                              , subq_7.ds__extract_day AS metric_time__extract_day
                              , subq_7.ds__extract_dow AS metric_time__extract_dow
                              , subq_7.ds__extract_doy AS metric_time__extract_doy
                              , subq_7.user
                              , subq_7.session
                              , subq_7.visit__user
                              , subq_7.visit__session
                              , subq_7.referrer_id
                              , subq_7.visit__referrer_id
                              , subq_7.visits
                              , subq_7.visitors
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
                            ) subq_7
                          ) subq_8
                        ) subq_9
                        INNER JOIN (
                          -- Add column with generated UUID
                          SELECT
                            subq_11.ds__day
                            , subq_11.ds__week
                            , subq_11.ds__month
                            , subq_11.ds__quarter
                            , subq_11.ds__year
                            , subq_11.ds__extract_year
                            , subq_11.ds__extract_quarter
                            , subq_11.ds__extract_month
                            , subq_11.ds__extract_day
                            , subq_11.ds__extract_dow
                            , subq_11.ds__extract_doy
                            , subq_11.buy__ds__day
                            , subq_11.buy__ds__week
                            , subq_11.buy__ds__month
                            , subq_11.buy__ds__quarter
                            , subq_11.buy__ds__year
                            , subq_11.buy__ds__extract_year
                            , subq_11.buy__ds__extract_quarter
                            , subq_11.buy__ds__extract_month
                            , subq_11.buy__ds__extract_day
                            , subq_11.buy__ds__extract_dow
                            , subq_11.buy__ds__extract_doy
                            , subq_11.metric_time__day
                            , subq_11.metric_time__week
                            , subq_11.metric_time__month
                            , subq_11.metric_time__quarter
                            , subq_11.metric_time__year
                            , subq_11.metric_time__extract_year
                            , subq_11.metric_time__extract_quarter
                            , subq_11.metric_time__extract_month
                            , subq_11.metric_time__extract_day
                            , subq_11.metric_time__extract_dow
                            , subq_11.metric_time__extract_doy
                            , subq_11.user
                            , subq_11.session_id
                            , subq_11.buy__user
                            , subq_11.buy__session_id
                            , subq_11.buys
                            , subq_11.buyers
                            , GEN_RANDOM_UUID() AS mf_internal_uuid
                          FROM (
                            -- Metric Time Dimension 'ds'
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
                              , subq_10.ds__day AS metric_time__day
                              , subq_10.ds__week AS metric_time__week
                              , subq_10.ds__month AS metric_time__month
                              , subq_10.ds__quarter AS metric_time__quarter
                              , subq_10.ds__year AS metric_time__year
                              , subq_10.ds__extract_year AS metric_time__extract_year
                              , subq_10.ds__extract_quarter AS metric_time__extract_quarter
                              , subq_10.ds__extract_month AS metric_time__extract_month
                              , subq_10.ds__extract_day AS metric_time__extract_day
                              , subq_10.ds__extract_dow AS metric_time__extract_dow
                              , subq_10.ds__extract_doy AS metric_time__extract_doy
                              , subq_10.user
                              , subq_10.session_id
                              , subq_10.buy__user
                              , subq_10.buy__session_id
                              , subq_10.buys
                              , subq_10.buyers
                            FROM (
                              -- Read Elements From Semantic Model 'buys_source'
                              SELECT
                                1 AS buys
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
                                , buys_source_src_28000.user_id AS user
                                , buys_source_src_28000.session_id
                                , buys_source_src_28000.user_id AS buy__user
                                , buys_source_src_28000.session_id AS buy__session_id
                              FROM ***************************.fct_buys buys_source_src_28000
                            ) subq_10
                          ) subq_11
                        ) subq_12
                        ON
                          (
                            subq_9.user = subq_12.user
                          ) AND (
                            (subq_9.ds__day <= subq_12.ds__day)
                          )
                      ) subq_13
                    ) subq_14
                  ) subq_15
                  GROUP BY
                    subq_15.user
                ) subq_16
                ON
                  subq_6.user = subq_16.user
                GROUP BY
                  COALESCE(subq_6.user, subq_16.user)
              ) subq_17
            ) subq_18
          ) subq_19
          ON
            subq_2.user = subq_19.user
        ) subq_20
      ) subq_21
      WHERE user__visit_buy_conversion_rate > 2
    ) subq_22
  ) subq_23
) subq_24
