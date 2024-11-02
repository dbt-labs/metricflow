-- Compute Metrics via Expressions
SELECT
  subq_13.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_12.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_11.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_10.user__revenue_all_time
        , subq_10.listings
      FROM (
        -- Pass Only Elements: ['listings', 'user__revenue_all_time']
        SELECT
          subq_9.user__revenue_all_time
          , subq_9.listings
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_8.user__revenue_all_time AS user__revenue_all_time
            , subq_2.user AS user
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
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS ds__day
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS ds__week
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS ds__month
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS ds__quarter
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS ds__year
                  , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                  , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                  , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                  , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS ds__extract_dow
                  , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS created_at__day
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS created_at__week
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS created_at__month
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS created_at__quarter
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS created_at__year
                  , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                  , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                  , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                  , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS created_at__extract_dow
                  , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                  , listings_latest_src_28000.country AS country_latest
                  , listings_latest_src_28000.is_lux AS is_lux_latest
                  , listings_latest_src_28000.capacity AS capacity_latest
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS listing__ds__day
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__ds__week
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS listing__ds__month
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__ds__quarter
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS listing__ds__year
                  , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                  , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                  , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                  , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__ds__extract_dow
                  , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, day) AS listing__created_at__day
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__created_at__week
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, month) AS listing__created_at__month
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__created_at__quarter
                  , DATETIME_TRUNC(listings_latest_src_28000.created_at, year) AS listing__created_at__year
                  , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_year
                  , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_quarter
                  , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_month
                  , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__created_at__extract_dow
                  , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__created_at__extract_doy
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
            -- Pass Only Elements: ['user', 'user__revenue_all_time']
            SELECT
              subq_7.user
              , subq_7.user__revenue_all_time
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_6.user
                , subq_6.txn_revenue AS user__revenue_all_time
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_5.user
                  , SUM(subq_5.txn_revenue) AS txn_revenue
                FROM (
                  -- Pass Only Elements: ['txn_revenue', 'user']
                  SELECT
                    subq_4.user
                    , subq_4.txn_revenue
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
                      , subq_3.revenue_instance__ds__day
                      , subq_3.revenue_instance__ds__week
                      , subq_3.revenue_instance__ds__month
                      , subq_3.revenue_instance__ds__quarter
                      , subq_3.revenue_instance__ds__year
                      , subq_3.revenue_instance__ds__extract_year
                      , subq_3.revenue_instance__ds__extract_quarter
                      , subq_3.revenue_instance__ds__extract_month
                      , subq_3.revenue_instance__ds__extract_day
                      , subq_3.revenue_instance__ds__extract_dow
                      , subq_3.revenue_instance__ds__extract_doy
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
                      , subq_3.revenue_instance__user
                      , subq_3.txn_revenue
                    FROM (
                      -- Read Elements From Semantic Model 'revenue'
                      SELECT
                        revenue_src_28000.revenue AS txn_revenue
                        , DATETIME_TRUNC(revenue_src_28000.created_at, day) AS ds__day
                        , DATETIME_TRUNC(revenue_src_28000.created_at, isoweek) AS ds__week
                        , DATETIME_TRUNC(revenue_src_28000.created_at, month) AS ds__month
                        , DATETIME_TRUNC(revenue_src_28000.created_at, quarter) AS ds__quarter
                        , DATETIME_TRUNC(revenue_src_28000.created_at, year) AS ds__year
                        , EXTRACT(year FROM revenue_src_28000.created_at) AS ds__extract_year
                        , EXTRACT(quarter FROM revenue_src_28000.created_at) AS ds__extract_quarter
                        , EXTRACT(month FROM revenue_src_28000.created_at) AS ds__extract_month
                        , EXTRACT(day FROM revenue_src_28000.created_at) AS ds__extract_day
                        , IF(EXTRACT(dayofweek FROM revenue_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM revenue_src_28000.created_at) - 1) AS ds__extract_dow
                        , EXTRACT(dayofyear FROM revenue_src_28000.created_at) AS ds__extract_doy
                        , DATETIME_TRUNC(revenue_src_28000.created_at, day) AS revenue_instance__ds__day
                        , DATETIME_TRUNC(revenue_src_28000.created_at, isoweek) AS revenue_instance__ds__week
                        , DATETIME_TRUNC(revenue_src_28000.created_at, month) AS revenue_instance__ds__month
                        , DATETIME_TRUNC(revenue_src_28000.created_at, quarter) AS revenue_instance__ds__quarter
                        , DATETIME_TRUNC(revenue_src_28000.created_at, year) AS revenue_instance__ds__year
                        , EXTRACT(year FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_year
                        , EXTRACT(quarter FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_quarter
                        , EXTRACT(month FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_month
                        , EXTRACT(day FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_day
                        , IF(EXTRACT(dayofweek FROM revenue_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM revenue_src_28000.created_at) - 1) AS revenue_instance__ds__extract_dow
                        , EXTRACT(dayofyear FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_doy
                        , revenue_src_28000.user_id AS user
                        , revenue_src_28000.user_id AS revenue_instance__user
                      FROM ***************************.fct_revenue revenue_src_28000
                    ) subq_3
                  ) subq_4
                ) subq_5
                GROUP BY
                  user
              ) subq_6
            ) subq_7
          ) subq_8
          ON
            subq_2.user = subq_8.user
        ) subq_9
      ) subq_10
      WHERE user__revenue_all_time > 1
    ) subq_11
  ) subq_12
) subq_13
