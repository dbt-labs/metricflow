test_name: test_query_with_cumulative_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a cumulative metric in the query-level where filter.

      Note this cumulative metric has no window / grain to date.
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_12.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(nr_subq_11.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      nr_subq_10.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        nr_subq_9.ds__day
        , nr_subq_9.ds__week
        , nr_subq_9.ds__month
        , nr_subq_9.ds__quarter
        , nr_subq_9.ds__year
        , nr_subq_9.ds__extract_year
        , nr_subq_9.ds__extract_quarter
        , nr_subq_9.ds__extract_month
        , nr_subq_9.ds__extract_day
        , nr_subq_9.ds__extract_dow
        , nr_subq_9.ds__extract_doy
        , nr_subq_9.created_at__day
        , nr_subq_9.created_at__week
        , nr_subq_9.created_at__month
        , nr_subq_9.created_at__quarter
        , nr_subq_9.created_at__year
        , nr_subq_9.created_at__extract_year
        , nr_subq_9.created_at__extract_quarter
        , nr_subq_9.created_at__extract_month
        , nr_subq_9.created_at__extract_day
        , nr_subq_9.created_at__extract_dow
        , nr_subq_9.created_at__extract_doy
        , nr_subq_9.listing__ds__day
        , nr_subq_9.listing__ds__week
        , nr_subq_9.listing__ds__month
        , nr_subq_9.listing__ds__quarter
        , nr_subq_9.listing__ds__year
        , nr_subq_9.listing__ds__extract_year
        , nr_subq_9.listing__ds__extract_quarter
        , nr_subq_9.listing__ds__extract_month
        , nr_subq_9.listing__ds__extract_day
        , nr_subq_9.listing__ds__extract_dow
        , nr_subq_9.listing__ds__extract_doy
        , nr_subq_9.listing__created_at__day
        , nr_subq_9.listing__created_at__week
        , nr_subq_9.listing__created_at__month
        , nr_subq_9.listing__created_at__quarter
        , nr_subq_9.listing__created_at__year
        , nr_subq_9.listing__created_at__extract_year
        , nr_subq_9.listing__created_at__extract_quarter
        , nr_subq_9.listing__created_at__extract_month
        , nr_subq_9.listing__created_at__extract_day
        , nr_subq_9.listing__created_at__extract_dow
        , nr_subq_9.listing__created_at__extract_doy
        , nr_subq_9.metric_time__day
        , nr_subq_9.metric_time__week
        , nr_subq_9.metric_time__month
        , nr_subq_9.metric_time__quarter
        , nr_subq_9.metric_time__year
        , nr_subq_9.metric_time__extract_year
        , nr_subq_9.metric_time__extract_quarter
        , nr_subq_9.metric_time__extract_month
        , nr_subq_9.metric_time__extract_day
        , nr_subq_9.metric_time__extract_dow
        , nr_subq_9.metric_time__extract_doy
        , nr_subq_9.listing
        , nr_subq_9.user
        , nr_subq_9.listing__user
        , nr_subq_9.country_latest
        , nr_subq_9.is_lux_latest
        , nr_subq_9.capacity_latest
        , nr_subq_9.listing__country_latest
        , nr_subq_9.listing__is_lux_latest
        , nr_subq_9.listing__capacity_latest
        , nr_subq_9.user__revenue_all_time
        , nr_subq_9.listings
        , nr_subq_9.largest_listing
        , nr_subq_9.smallest_listing
      FROM (
        -- Join Standard Outputs
        SELECT
          nr_subq_8.user__revenue_all_time AS user__revenue_all_time
          , nr_subq_3.ds__day AS ds__day
          , nr_subq_3.ds__week AS ds__week
          , nr_subq_3.ds__month AS ds__month
          , nr_subq_3.ds__quarter AS ds__quarter
          , nr_subq_3.ds__year AS ds__year
          , nr_subq_3.ds__extract_year AS ds__extract_year
          , nr_subq_3.ds__extract_quarter AS ds__extract_quarter
          , nr_subq_3.ds__extract_month AS ds__extract_month
          , nr_subq_3.ds__extract_day AS ds__extract_day
          , nr_subq_3.ds__extract_dow AS ds__extract_dow
          , nr_subq_3.ds__extract_doy AS ds__extract_doy
          , nr_subq_3.created_at__day AS created_at__day
          , nr_subq_3.created_at__week AS created_at__week
          , nr_subq_3.created_at__month AS created_at__month
          , nr_subq_3.created_at__quarter AS created_at__quarter
          , nr_subq_3.created_at__year AS created_at__year
          , nr_subq_3.created_at__extract_year AS created_at__extract_year
          , nr_subq_3.created_at__extract_quarter AS created_at__extract_quarter
          , nr_subq_3.created_at__extract_month AS created_at__extract_month
          , nr_subq_3.created_at__extract_day AS created_at__extract_day
          , nr_subq_3.created_at__extract_dow AS created_at__extract_dow
          , nr_subq_3.created_at__extract_doy AS created_at__extract_doy
          , nr_subq_3.listing__ds__day AS listing__ds__day
          , nr_subq_3.listing__ds__week AS listing__ds__week
          , nr_subq_3.listing__ds__month AS listing__ds__month
          , nr_subq_3.listing__ds__quarter AS listing__ds__quarter
          , nr_subq_3.listing__ds__year AS listing__ds__year
          , nr_subq_3.listing__ds__extract_year AS listing__ds__extract_year
          , nr_subq_3.listing__ds__extract_quarter AS listing__ds__extract_quarter
          , nr_subq_3.listing__ds__extract_month AS listing__ds__extract_month
          , nr_subq_3.listing__ds__extract_day AS listing__ds__extract_day
          , nr_subq_3.listing__ds__extract_dow AS listing__ds__extract_dow
          , nr_subq_3.listing__ds__extract_doy AS listing__ds__extract_doy
          , nr_subq_3.listing__created_at__day AS listing__created_at__day
          , nr_subq_3.listing__created_at__week AS listing__created_at__week
          , nr_subq_3.listing__created_at__month AS listing__created_at__month
          , nr_subq_3.listing__created_at__quarter AS listing__created_at__quarter
          , nr_subq_3.listing__created_at__year AS listing__created_at__year
          , nr_subq_3.listing__created_at__extract_year AS listing__created_at__extract_year
          , nr_subq_3.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
          , nr_subq_3.listing__created_at__extract_month AS listing__created_at__extract_month
          , nr_subq_3.listing__created_at__extract_day AS listing__created_at__extract_day
          , nr_subq_3.listing__created_at__extract_dow AS listing__created_at__extract_dow
          , nr_subq_3.listing__created_at__extract_doy AS listing__created_at__extract_doy
          , nr_subq_3.metric_time__day AS metric_time__day
          , nr_subq_3.metric_time__week AS metric_time__week
          , nr_subq_3.metric_time__month AS metric_time__month
          , nr_subq_3.metric_time__quarter AS metric_time__quarter
          , nr_subq_3.metric_time__year AS metric_time__year
          , nr_subq_3.metric_time__extract_year AS metric_time__extract_year
          , nr_subq_3.metric_time__extract_quarter AS metric_time__extract_quarter
          , nr_subq_3.metric_time__extract_month AS metric_time__extract_month
          , nr_subq_3.metric_time__extract_day AS metric_time__extract_day
          , nr_subq_3.metric_time__extract_dow AS metric_time__extract_dow
          , nr_subq_3.metric_time__extract_doy AS metric_time__extract_doy
          , nr_subq_3.listing AS listing
          , nr_subq_3.user AS user
          , nr_subq_3.listing__user AS listing__user
          , nr_subq_3.country_latest AS country_latest
          , nr_subq_3.is_lux_latest AS is_lux_latest
          , nr_subq_3.capacity_latest AS capacity_latest
          , nr_subq_3.listing__country_latest AS listing__country_latest
          , nr_subq_3.listing__is_lux_latest AS listing__is_lux_latest
          , nr_subq_3.listing__capacity_latest AS listing__capacity_latest
          , nr_subq_3.listings AS listings
          , nr_subq_3.largest_listing AS largest_listing
          , nr_subq_3.smallest_listing AS smallest_listing
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
          ) nr_subq_28007
        ) nr_subq_3
        LEFT OUTER JOIN (
          -- Pass Only Elements: ['user', 'user__revenue_all_time']
          SELECT
            nr_subq_7.user
            , nr_subq_7.user__revenue_all_time
          FROM (
            -- Compute Metrics via Expressions
            SELECT
              nr_subq_6.user
              , nr_subq_6.txn_revenue AS user__revenue_all_time
            FROM (
              -- Aggregate Measures
              SELECT
                nr_subq_5.user
                , SUM(nr_subq_5.txn_revenue) AS txn_revenue
              FROM (
                -- Pass Only Elements: ['txn_revenue', 'user']
                SELECT
                  nr_subq_4.user
                  , nr_subq_4.txn_revenue
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    nr_subq_28008.ds__day
                    , nr_subq_28008.ds__week
                    , nr_subq_28008.ds__month
                    , nr_subq_28008.ds__quarter
                    , nr_subq_28008.ds__year
                    , nr_subq_28008.ds__extract_year
                    , nr_subq_28008.ds__extract_quarter
                    , nr_subq_28008.ds__extract_month
                    , nr_subq_28008.ds__extract_day
                    , nr_subq_28008.ds__extract_dow
                    , nr_subq_28008.ds__extract_doy
                    , nr_subq_28008.revenue_instance__ds__day
                    , nr_subq_28008.revenue_instance__ds__week
                    , nr_subq_28008.revenue_instance__ds__month
                    , nr_subq_28008.revenue_instance__ds__quarter
                    , nr_subq_28008.revenue_instance__ds__year
                    , nr_subq_28008.revenue_instance__ds__extract_year
                    , nr_subq_28008.revenue_instance__ds__extract_quarter
                    , nr_subq_28008.revenue_instance__ds__extract_month
                    , nr_subq_28008.revenue_instance__ds__extract_day
                    , nr_subq_28008.revenue_instance__ds__extract_dow
                    , nr_subq_28008.revenue_instance__ds__extract_doy
                    , nr_subq_28008.ds__day AS metric_time__day
                    , nr_subq_28008.ds__week AS metric_time__week
                    , nr_subq_28008.ds__month AS metric_time__month
                    , nr_subq_28008.ds__quarter AS metric_time__quarter
                    , nr_subq_28008.ds__year AS metric_time__year
                    , nr_subq_28008.ds__extract_year AS metric_time__extract_year
                    , nr_subq_28008.ds__extract_quarter AS metric_time__extract_quarter
                    , nr_subq_28008.ds__extract_month AS metric_time__extract_month
                    , nr_subq_28008.ds__extract_day AS metric_time__extract_day
                    , nr_subq_28008.ds__extract_dow AS metric_time__extract_dow
                    , nr_subq_28008.ds__extract_doy AS metric_time__extract_doy
                    , nr_subq_28008.user
                    , nr_subq_28008.revenue_instance__user
                    , nr_subq_28008.txn_revenue
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
                  ) nr_subq_28008
                ) nr_subq_4
              ) nr_subq_5
              GROUP BY
                user
            ) nr_subq_6
          ) nr_subq_7
        ) nr_subq_8
        ON
          nr_subq_3.user = nr_subq_8.user
      ) nr_subq_9
      WHERE user__revenue_all_time > 1
    ) nr_subq_10
  ) nr_subq_11
) nr_subq_12
