-- Compute Metrics via Expressions
SELECT
  subq_39.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_38.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_37.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_36.user__visit_buy_conversion_rate
        , subq_36.listings
      FROM (
        -- Pass Only Elements: ['listings', 'user__visit_buy_conversion_rate']
        SELECT
          subq_35.user__visit_buy_conversion_rate
          , subq_35.listings
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_17.user AS user
            , subq_34.user__visit_buy_conversion_rate AS user__visit_buy_conversion_rate
            , subq_17.listings AS listings
          FROM (
            -- Pass Only Elements: ['listings', 'user']
            SELECT
              subq_16.user
              , subq_16.listings
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
                , subq_15.listings
                , subq_15.largest_listing
                , subq_15.smallest_listing
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
                  , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS ds__extract_dow
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
                  , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS created_at__extract_dow
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
                  , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS listing__ds__extract_dow
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
                  , EXTRACT(DAYOFWEEK_ISO FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_dow
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
          ) subq_17
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['user', 'user__visit_buy_conversion_rate']
            SELECT
              subq_33.user
              , subq_33.user__visit_buy_conversion_rate
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_32.user
                , CAST(subq_32.buys AS DOUBLE) / CAST(NULLIF(subq_32.visits, 0) AS DOUBLE) AS user__visit_buy_conversion_rate
              FROM (
                -- Combine Aggregated Outputs
                SELECT
                  COALESCE(subq_21.user, subq_31.user) AS user
                  , MAX(subq_21.visits) AS visits
                  , MAX(subq_31.buys) AS buys
                FROM (
                  -- Aggregate Measures
                  SELECT
                    subq_20.user
                    , SUM(subq_20.visits) AS visits
                  FROM (
                    -- Pass Only Elements: ['visits', 'user']
                    SELECT
                      subq_19.user
                      , subq_19.visits
                    FROM (
                      -- Metric Time Dimension 'ds'
                      SELECT
                        subq_18.ds__day
                        , subq_18.ds__week
                        , subq_18.ds__month
                        , subq_18.ds__quarter
                        , subq_18.ds__year
                        , subq_18.ds__extract_year
                        , subq_18.ds__extract_quarter
                        , subq_18.ds__extract_month
                        , subq_18.ds__extract_day
                        , subq_18.ds__extract_dow
                        , subq_18.ds__extract_doy
                        , subq_18.visit__ds__day
                        , subq_18.visit__ds__week
                        , subq_18.visit__ds__month
                        , subq_18.visit__ds__quarter
                        , subq_18.visit__ds__year
                        , subq_18.visit__ds__extract_year
                        , subq_18.visit__ds__extract_quarter
                        , subq_18.visit__ds__extract_month
                        , subq_18.visit__ds__extract_day
                        , subq_18.visit__ds__extract_dow
                        , subq_18.visit__ds__extract_doy
                        , subq_18.ds__day AS metric_time__day
                        , subq_18.ds__week AS metric_time__week
                        , subq_18.ds__month AS metric_time__month
                        , subq_18.ds__quarter AS metric_time__quarter
                        , subq_18.ds__year AS metric_time__year
                        , subq_18.ds__extract_year AS metric_time__extract_year
                        , subq_18.ds__extract_quarter AS metric_time__extract_quarter
                        , subq_18.ds__extract_month AS metric_time__extract_month
                        , subq_18.ds__extract_day AS metric_time__extract_day
                        , subq_18.ds__extract_dow AS metric_time__extract_dow
                        , subq_18.ds__extract_doy AS metric_time__extract_doy
                        , subq_18.user
                        , subq_18.session
                        , subq_18.visit__user
                        , subq_18.visit__session
                        , subq_18.referrer_id
                        , subq_18.visit__referrer_id
                        , subq_18.visits
                        , subq_18.visitors
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
                          , EXTRACT(DAYOFWEEK_ISO FROM visits_source_src_28000.ds) AS ds__extract_dow
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
                          , EXTRACT(DAYOFWEEK_ISO FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                          , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                          , visits_source_src_28000.referrer_id AS visit__referrer_id
                          , visits_source_src_28000.user_id AS user
                          , visits_source_src_28000.session_id AS session
                          , visits_source_src_28000.user_id AS visit__user
                          , visits_source_src_28000.session_id AS visit__session
                        FROM ***************************.fct_visits visits_source_src_28000
                      ) subq_18
                    ) subq_19
                  ) subq_20
                  GROUP BY
                    subq_20.user
                ) subq_21
                FULL OUTER JOIN (
                  -- Aggregate Measures
                  SELECT
                    subq_30.user
                    , SUM(subq_30.buys) AS buys
                  FROM (
                    -- Pass Only Elements: ['buys', 'user']
                    SELECT
                      subq_29.user
                      , subq_29.buys
                    FROM (
                      -- Find conversions for user within the range of INF
                      SELECT
                        subq_28.ds__day
                        , subq_28.user
                        , subq_28.buys
                        , subq_28.visits
                      FROM (
                        -- Dedupe the fanout with mf_internal_uuid in the conversion data set
                        SELECT DISTINCT
                          first_value(subq_24.visits) OVER (
                            PARTITION BY
                              subq_27.user
                              , subq_27.ds__day
                              , subq_27.mf_internal_uuid
                            ORDER BY subq_24.ds__day DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                          ) AS visits
                          , first_value(subq_24.ds__day) OVER (
                            PARTITION BY
                              subq_27.user
                              , subq_27.ds__day
                              , subq_27.mf_internal_uuid
                            ORDER BY subq_24.ds__day DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                          ) AS ds__day
                          , first_value(subq_24.user) OVER (
                            PARTITION BY
                              subq_27.user
                              , subq_27.ds__day
                              , subq_27.mf_internal_uuid
                            ORDER BY subq_24.ds__day DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                          ) AS user
                          , subq_27.mf_internal_uuid AS mf_internal_uuid
                          , subq_27.buys AS buys
                        FROM (
                          -- Pass Only Elements: ['visits', 'ds__day', 'user']
                          SELECT
                            subq_23.ds__day
                            , subq_23.user
                            , subq_23.visits
                          FROM (
                            -- Metric Time Dimension 'ds'
                            SELECT
                              subq_22.ds__day
                              , subq_22.ds__week
                              , subq_22.ds__month
                              , subq_22.ds__quarter
                              , subq_22.ds__year
                              , subq_22.ds__extract_year
                              , subq_22.ds__extract_quarter
                              , subq_22.ds__extract_month
                              , subq_22.ds__extract_day
                              , subq_22.ds__extract_dow
                              , subq_22.ds__extract_doy
                              , subq_22.visit__ds__day
                              , subq_22.visit__ds__week
                              , subq_22.visit__ds__month
                              , subq_22.visit__ds__quarter
                              , subq_22.visit__ds__year
                              , subq_22.visit__ds__extract_year
                              , subq_22.visit__ds__extract_quarter
                              , subq_22.visit__ds__extract_month
                              , subq_22.visit__ds__extract_day
                              , subq_22.visit__ds__extract_dow
                              , subq_22.visit__ds__extract_doy
                              , subq_22.ds__day AS metric_time__day
                              , subq_22.ds__week AS metric_time__week
                              , subq_22.ds__month AS metric_time__month
                              , subq_22.ds__quarter AS metric_time__quarter
                              , subq_22.ds__year AS metric_time__year
                              , subq_22.ds__extract_year AS metric_time__extract_year
                              , subq_22.ds__extract_quarter AS metric_time__extract_quarter
                              , subq_22.ds__extract_month AS metric_time__extract_month
                              , subq_22.ds__extract_day AS metric_time__extract_day
                              , subq_22.ds__extract_dow AS metric_time__extract_dow
                              , subq_22.ds__extract_doy AS metric_time__extract_doy
                              , subq_22.user
                              , subq_22.session
                              , subq_22.visit__user
                              , subq_22.visit__session
                              , subq_22.referrer_id
                              , subq_22.visit__referrer_id
                              , subq_22.visits
                              , subq_22.visitors
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
                                , EXTRACT(DAYOFWEEK_ISO FROM visits_source_src_28000.ds) AS ds__extract_dow
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
                                , EXTRACT(DAYOFWEEK_ISO FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                                , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                                , visits_source_src_28000.referrer_id AS visit__referrer_id
                                , visits_source_src_28000.user_id AS user
                                , visits_source_src_28000.session_id AS session
                                , visits_source_src_28000.user_id AS visit__user
                                , visits_source_src_28000.session_id AS visit__session
                              FROM ***************************.fct_visits visits_source_src_28000
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
                            , subq_26.buys
                            , subq_26.buyers
                            , UUID() AS mf_internal_uuid
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
                              , subq_25.buys
                              , subq_25.buyers
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
                                , EXTRACT(DAYOFWEEK_ISO FROM buys_source_src_28000.ds) AS ds__extract_dow
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
                                , EXTRACT(DAYOFWEEK_ISO FROM buys_source_src_28000.ds) AS buy__ds__extract_dow
                                , EXTRACT(doy FROM buys_source_src_28000.ds) AS buy__ds__extract_doy
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
                            (subq_24.ds__day <= subq_27.ds__day)
                          )
                      ) subq_28
                    ) subq_29
                  ) subq_30
                  GROUP BY
                    subq_30.user
                ) subq_31
                ON
                  subq_21.user = subq_31.user
                GROUP BY
                  COALESCE(subq_21.user, subq_31.user)
              ) subq_32
            ) subq_33
          ) subq_34
          ON
            subq_17.user = subq_34.user
        ) subq_35
      ) subq_36
      WHERE user__visit_buy_conversion_rate > 2
    ) subq_37
  ) subq_38
) subq_39
