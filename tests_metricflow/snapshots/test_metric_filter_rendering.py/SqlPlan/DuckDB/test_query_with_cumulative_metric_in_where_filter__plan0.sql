test_name: test_query_with_cumulative_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a cumulative metric in the query-level where filter.

      Note this cumulative metric has no window / grain to date.
sql_engine: DuckDB
---
-- Write to DataTable
SELECT
  subq_17.listings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_16.listings
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      SUM(subq_15.listings) AS listings
    FROM (
      -- Pass Only Elements: ['listings']
      SELECT
        subq_14.listings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_13.ds__day
          , subq_13.ds__week
          , subq_13.ds__month
          , subq_13.ds__quarter
          , subq_13.ds__year
          , subq_13.ds__extract_year
          , subq_13.ds__extract_quarter
          , subq_13.ds__extract_month
          , subq_13.ds__extract_day
          , subq_13.ds__extract_dow
          , subq_13.ds__extract_doy
          , subq_13.created_at__day
          , subq_13.created_at__week
          , subq_13.created_at__month
          , subq_13.created_at__quarter
          , subq_13.created_at__year
          , subq_13.created_at__extract_year
          , subq_13.created_at__extract_quarter
          , subq_13.created_at__extract_month
          , subq_13.created_at__extract_day
          , subq_13.created_at__extract_dow
          , subq_13.created_at__extract_doy
          , subq_13.listing__ds__day
          , subq_13.listing__ds__week
          , subq_13.listing__ds__month
          , subq_13.listing__ds__quarter
          , subq_13.listing__ds__year
          , subq_13.listing__ds__extract_year
          , subq_13.listing__ds__extract_quarter
          , subq_13.listing__ds__extract_month
          , subq_13.listing__ds__extract_day
          , subq_13.listing__ds__extract_dow
          , subq_13.listing__ds__extract_doy
          , subq_13.listing__created_at__day
          , subq_13.listing__created_at__week
          , subq_13.listing__created_at__month
          , subq_13.listing__created_at__quarter
          , subq_13.listing__created_at__year
          , subq_13.listing__created_at__extract_year
          , subq_13.listing__created_at__extract_quarter
          , subq_13.listing__created_at__extract_month
          , subq_13.listing__created_at__extract_day
          , subq_13.listing__created_at__extract_dow
          , subq_13.listing__created_at__extract_doy
          , subq_13.metric_time__day
          , subq_13.metric_time__week
          , subq_13.metric_time__month
          , subq_13.metric_time__quarter
          , subq_13.metric_time__year
          , subq_13.metric_time__extract_year
          , subq_13.metric_time__extract_quarter
          , subq_13.metric_time__extract_month
          , subq_13.metric_time__extract_day
          , subq_13.metric_time__extract_dow
          , subq_13.metric_time__extract_doy
          , subq_13.listing
          , subq_13.user
          , subq_13.listing__user
          , subq_13.country_latest
          , subq_13.is_lux_latest
          , subq_13.capacity_latest
          , subq_13.listing__country_latest
          , subq_13.listing__is_lux_latest
          , subq_13.listing__capacity_latest
          , subq_13.user__revenue_all_time
          , subq_13.active_listings
          , subq_13.largest_listing
          , subq_13.listings
          , subq_13.lux_listings
          , subq_13.smallest_listing
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_12.user__revenue_all_time AS user__revenue_all_time
            , subq_5.ds__day AS ds__day
            , subq_5.ds__week AS ds__week
            , subq_5.ds__month AS ds__month
            , subq_5.ds__quarter AS ds__quarter
            , subq_5.ds__year AS ds__year
            , subq_5.ds__extract_year AS ds__extract_year
            , subq_5.ds__extract_quarter AS ds__extract_quarter
            , subq_5.ds__extract_month AS ds__extract_month
            , subq_5.ds__extract_day AS ds__extract_day
            , subq_5.ds__extract_dow AS ds__extract_dow
            , subq_5.ds__extract_doy AS ds__extract_doy
            , subq_5.created_at__day AS created_at__day
            , subq_5.created_at__week AS created_at__week
            , subq_5.created_at__month AS created_at__month
            , subq_5.created_at__quarter AS created_at__quarter
            , subq_5.created_at__year AS created_at__year
            , subq_5.created_at__extract_year AS created_at__extract_year
            , subq_5.created_at__extract_quarter AS created_at__extract_quarter
            , subq_5.created_at__extract_month AS created_at__extract_month
            , subq_5.created_at__extract_day AS created_at__extract_day
            , subq_5.created_at__extract_dow AS created_at__extract_dow
            , subq_5.created_at__extract_doy AS created_at__extract_doy
            , subq_5.listing__ds__day AS listing__ds__day
            , subq_5.listing__ds__week AS listing__ds__week
            , subq_5.listing__ds__month AS listing__ds__month
            , subq_5.listing__ds__quarter AS listing__ds__quarter
            , subq_5.listing__ds__year AS listing__ds__year
            , subq_5.listing__ds__extract_year AS listing__ds__extract_year
            , subq_5.listing__ds__extract_quarter AS listing__ds__extract_quarter
            , subq_5.listing__ds__extract_month AS listing__ds__extract_month
            , subq_5.listing__ds__extract_day AS listing__ds__extract_day
            , subq_5.listing__ds__extract_dow AS listing__ds__extract_dow
            , subq_5.listing__ds__extract_doy AS listing__ds__extract_doy
            , subq_5.listing__created_at__day AS listing__created_at__day
            , subq_5.listing__created_at__week AS listing__created_at__week
            , subq_5.listing__created_at__month AS listing__created_at__month
            , subq_5.listing__created_at__quarter AS listing__created_at__quarter
            , subq_5.listing__created_at__year AS listing__created_at__year
            , subq_5.listing__created_at__extract_year AS listing__created_at__extract_year
            , subq_5.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
            , subq_5.listing__created_at__extract_month AS listing__created_at__extract_month
            , subq_5.listing__created_at__extract_day AS listing__created_at__extract_day
            , subq_5.listing__created_at__extract_dow AS listing__created_at__extract_dow
            , subq_5.listing__created_at__extract_doy AS listing__created_at__extract_doy
            , subq_5.metric_time__day AS metric_time__day
            , subq_5.metric_time__week AS metric_time__week
            , subq_5.metric_time__month AS metric_time__month
            , subq_5.metric_time__quarter AS metric_time__quarter
            , subq_5.metric_time__year AS metric_time__year
            , subq_5.metric_time__extract_year AS metric_time__extract_year
            , subq_5.metric_time__extract_quarter AS metric_time__extract_quarter
            , subq_5.metric_time__extract_month AS metric_time__extract_month
            , subq_5.metric_time__extract_day AS metric_time__extract_day
            , subq_5.metric_time__extract_dow AS metric_time__extract_dow
            , subq_5.metric_time__extract_doy AS metric_time__extract_doy
            , subq_5.listing AS listing
            , subq_5.user AS user
            , subq_5.listing__user AS listing__user
            , subq_5.country_latest AS country_latest
            , subq_5.is_lux_latest AS is_lux_latest
            , subq_5.capacity_latest AS capacity_latest
            , subq_5.listing__country_latest AS listing__country_latest
            , subq_5.listing__is_lux_latest AS listing__is_lux_latest
            , subq_5.listing__capacity_latest AS listing__capacity_latest
            , subq_5.active_listings AS active_listings
            , subq_5.largest_listing AS largest_listing
            , subq_5.listings AS listings
            , subq_5.lux_listings AS lux_listings
            , subq_5.smallest_listing AS smallest_listing
          FROM (
            -- Metric Time Dimension 'ds'
            SELECT
              subq_4.ds__day
              , subq_4.ds__week
              , subq_4.ds__month
              , subq_4.ds__quarter
              , subq_4.ds__year
              , subq_4.ds__extract_year
              , subq_4.ds__extract_quarter
              , subq_4.ds__extract_month
              , subq_4.ds__extract_day
              , subq_4.ds__extract_dow
              , subq_4.ds__extract_doy
              , subq_4.created_at__day
              , subq_4.created_at__week
              , subq_4.created_at__month
              , subq_4.created_at__quarter
              , subq_4.created_at__year
              , subq_4.created_at__extract_year
              , subq_4.created_at__extract_quarter
              , subq_4.created_at__extract_month
              , subq_4.created_at__extract_day
              , subq_4.created_at__extract_dow
              , subq_4.created_at__extract_doy
              , subq_4.listing__ds__day
              , subq_4.listing__ds__week
              , subq_4.listing__ds__month
              , subq_4.listing__ds__quarter
              , subq_4.listing__ds__year
              , subq_4.listing__ds__extract_year
              , subq_4.listing__ds__extract_quarter
              , subq_4.listing__ds__extract_month
              , subq_4.listing__ds__extract_day
              , subq_4.listing__ds__extract_dow
              , subq_4.listing__ds__extract_doy
              , subq_4.listing__created_at__day
              , subq_4.listing__created_at__week
              , subq_4.listing__created_at__month
              , subq_4.listing__created_at__quarter
              , subq_4.listing__created_at__year
              , subq_4.listing__created_at__extract_year
              , subq_4.listing__created_at__extract_quarter
              , subq_4.listing__created_at__extract_month
              , subq_4.listing__created_at__extract_day
              , subq_4.listing__created_at__extract_dow
              , subq_4.listing__created_at__extract_doy
              , subq_4.ds__day AS metric_time__day
              , subq_4.ds__week AS metric_time__week
              , subq_4.ds__month AS metric_time__month
              , subq_4.ds__quarter AS metric_time__quarter
              , subq_4.ds__year AS metric_time__year
              , subq_4.ds__extract_year AS metric_time__extract_year
              , subq_4.ds__extract_quarter AS metric_time__extract_quarter
              , subq_4.ds__extract_month AS metric_time__extract_month
              , subq_4.ds__extract_day AS metric_time__extract_day
              , subq_4.ds__extract_dow AS metric_time__extract_dow
              , subq_4.ds__extract_doy AS metric_time__extract_doy
              , subq_4.listing
              , subq_4.user
              , subq_4.listing__user
              , subq_4.country_latest
              , subq_4.is_lux_latest
              , subq_4.capacity_latest
              , subq_4.listing__country_latest
              , subq_4.listing__is_lux_latest
              , subq_4.listing__capacity_latest
              , subq_4.active_listings
              , subq_4.largest_listing
              , subq_4.listings
              , subq_4.lux_listings
              , subq_4.smallest_listing
            FROM (
              -- Read Elements From Semantic Model 'listings_latest'
              SELECT
                1 AS active_listings
                , listings_latest_src_28000.capacity AS largest_listing
                , 1 AS listings
                , 1 AS lux_listings
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
            ) subq_4
          ) subq_5
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['user', 'user__revenue_all_time']
            SELECT
              subq_11.user
              , subq_11.user__revenue_all_time
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_10.user
                , subq_10.revenue AS user__revenue_all_time
              FROM (
                -- Compute Metrics via Expressions
                SELECT
                  subq_9.user
                  , subq_9.revenue
                FROM (
                  -- Aggregate Inputs for Simple Metrics
                  SELECT
                    subq_8.user
                    , SUM(subq_8.revenue) AS revenue
                  FROM (
                    -- Pass Only Elements: ['revenue', 'user']
                    SELECT
                      subq_7.user
                      , subq_7.revenue
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
                        , subq_6.revenue_instance__ds__day
                        , subq_6.revenue_instance__ds__week
                        , subq_6.revenue_instance__ds__month
                        , subq_6.revenue_instance__ds__quarter
                        , subq_6.revenue_instance__ds__year
                        , subq_6.revenue_instance__ds__extract_year
                        , subq_6.revenue_instance__ds__extract_quarter
                        , subq_6.revenue_instance__ds__extract_month
                        , subq_6.revenue_instance__ds__extract_day
                        , subq_6.revenue_instance__ds__extract_dow
                        , subq_6.revenue_instance__ds__extract_doy
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
                        , subq_6.revenue_instance__user
                        , subq_6.revenue
                      FROM (
                        -- Read Elements From Semantic Model 'revenue'
                        SELECT
                          revenue_src_28000.revenue
                          , DATE_TRUNC('day', revenue_src_28000.created_at) AS ds__day
                          , DATE_TRUNC('week', revenue_src_28000.created_at) AS ds__week
                          , DATE_TRUNC('month', revenue_src_28000.created_at) AS ds__month
                          , DATE_TRUNC('quarter', revenue_src_28000.created_at) AS ds__quarter
                          , DATE_TRUNC('year', revenue_src_28000.created_at) AS ds__year
                          , EXTRACT(year FROM revenue_src_28000.created_at) AS ds__extract_year
                          , EXTRACT(quarter FROM revenue_src_28000.created_at) AS ds__extract_quarter
                          , EXTRACT(month FROM revenue_src_28000.created_at) AS ds__extract_month
                          , EXTRACT(day FROM revenue_src_28000.created_at) AS ds__extract_day
                          , EXTRACT(isodow FROM revenue_src_28000.created_at) AS ds__extract_dow
                          , EXTRACT(doy FROM revenue_src_28000.created_at) AS ds__extract_doy
                          , DATE_TRUNC('day', revenue_src_28000.created_at) AS revenue_instance__ds__day
                          , DATE_TRUNC('week', revenue_src_28000.created_at) AS revenue_instance__ds__week
                          , DATE_TRUNC('month', revenue_src_28000.created_at) AS revenue_instance__ds__month
                          , DATE_TRUNC('quarter', revenue_src_28000.created_at) AS revenue_instance__ds__quarter
                          , DATE_TRUNC('year', revenue_src_28000.created_at) AS revenue_instance__ds__year
                          , EXTRACT(year FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_year
                          , EXTRACT(quarter FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_quarter
                          , EXTRACT(month FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_month
                          , EXTRACT(day FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_day
                          , EXTRACT(isodow FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_dow
                          , EXTRACT(doy FROM revenue_src_28000.created_at) AS revenue_instance__ds__extract_doy
                          , revenue_src_28000.user_id AS user
                          , revenue_src_28000.user_id AS revenue_instance__user
                        FROM ***************************.fct_revenue revenue_src_28000
                      ) subq_6
                    ) subq_7
                  ) subq_8
                  GROUP BY
                    subq_8.user
                ) subq_9
              ) subq_10
            ) subq_11
          ) subq_12
          ON
            subq_5.user = subq_12.user
        ) subq_13
        WHERE user__revenue_all_time > 1
      ) subq_14
    ) subq_15
  ) subq_16
) subq_17
