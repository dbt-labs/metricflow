-- Compute Metrics via Expressions
SELECT
  subq_11.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_10.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_9.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_8.ds__day
        , subq_8.ds__week
        , subq_8.ds__month
        , subq_8.ds__quarter
        , subq_8.ds__year
        , subq_8.ds__extract_year
        , subq_8.ds__extract_quarter
        , subq_8.ds__extract_month
        , subq_8.ds__extract_day
        , subq_8.ds__extract_dow
        , subq_8.ds__extract_doy
        , subq_8.created_at__day
        , subq_8.created_at__week
        , subq_8.created_at__month
        , subq_8.created_at__quarter
        , subq_8.created_at__year
        , subq_8.created_at__extract_year
        , subq_8.created_at__extract_quarter
        , subq_8.created_at__extract_month
        , subq_8.created_at__extract_day
        , subq_8.created_at__extract_dow
        , subq_8.created_at__extract_doy
        , subq_8.listing__ds__day
        , subq_8.listing__ds__week
        , subq_8.listing__ds__month
        , subq_8.listing__ds__quarter
        , subq_8.listing__ds__year
        , subq_8.listing__ds__extract_year
        , subq_8.listing__ds__extract_quarter
        , subq_8.listing__ds__extract_month
        , subq_8.listing__ds__extract_day
        , subq_8.listing__ds__extract_dow
        , subq_8.listing__ds__extract_doy
        , subq_8.listing__created_at__day
        , subq_8.listing__created_at__week
        , subq_8.listing__created_at__month
        , subq_8.listing__created_at__quarter
        , subq_8.listing__created_at__year
        , subq_8.listing__created_at__extract_year
        , subq_8.listing__created_at__extract_quarter
        , subq_8.listing__created_at__extract_month
        , subq_8.listing__created_at__extract_day
        , subq_8.listing__created_at__extract_dow
        , subq_8.listing__created_at__extract_doy
        , subq_8.metric_time__day
        , subq_8.metric_time__week
        , subq_8.metric_time__month
        , subq_8.metric_time__quarter
        , subq_8.metric_time__year
        , subq_8.metric_time__extract_year
        , subq_8.metric_time__extract_quarter
        , subq_8.metric_time__extract_month
        , subq_8.metric_time__extract_day
        , subq_8.metric_time__extract_dow
        , subq_8.metric_time__extract_doy
        , subq_8.listing
        , subq_8.user
        , subq_8.listing__user
        , subq_8.country_latest
        , subq_8.is_lux_latest
        , subq_8.capacity_latest
        , subq_8.listing__country_latest
        , subq_8.listing__is_lux_latest
        , subq_8.listing__capacity_latest
        , subq_8.user__revenue_all_time
        , subq_8.listings
        , subq_8.largest_listing
        , subq_8.smallest_listing
      FROM (
        -- Join Standard Outputs
        SELECT
          subq_7.user__revenue_all_time AS user__revenue_all_time
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
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['user', 'user__revenue_all_time']
          SELECT
            subq_6.user
            , subq_6.user__revenue_all_time
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              subq_5.user
              , subq_5.txn_revenue AS user__revenue_all_time
            FROM (
              -- Aggregate Measures
              SELECT
                subq_4.user
                , SUM(subq_4.txn_revenue) AS txn_revenue
              FROM (
                -- Pass Only Elements: ['txn_revenue', 'user']
                SELECT
                  subq_3.user
                  , subq_3.txn_revenue
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
                    , subq_2.revenue_instance__ds__day
                    , subq_2.revenue_instance__ds__week
                    , subq_2.revenue_instance__ds__month
                    , subq_2.revenue_instance__ds__quarter
                    , subq_2.revenue_instance__ds__year
                    , subq_2.revenue_instance__ds__extract_year
                    , subq_2.revenue_instance__ds__extract_quarter
                    , subq_2.revenue_instance__ds__extract_month
                    , subq_2.revenue_instance__ds__extract_day
                    , subq_2.revenue_instance__ds__extract_dow
                    , subq_2.revenue_instance__ds__extract_doy
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
                    , subq_2.revenue_instance__user
                    , subq_2.txn_revenue
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
                  ) subq_2
                ) subq_3
              ) subq_4
              GROUP BY
                user
            ) subq_5
          ) subq_6
        ) subq_7
        ON
          subq_1.user = subq_7.user
      ) subq_8
      WHERE user__revenue_all_time > 1
    ) subq_9
  ) subq_10
) subq_11
