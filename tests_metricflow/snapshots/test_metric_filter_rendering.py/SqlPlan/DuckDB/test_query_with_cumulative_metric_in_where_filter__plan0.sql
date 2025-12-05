test_name: test_query_with_cumulative_metric_in_where_filter
test_filename: test_metric_filter_rendering.py
docstring:
  Tests a query with a cumulative metric in the query-level where filter.

      Note this cumulative metric has no window / grain to date.
sql_engine: DuckDB
---
-- Write to DataTable
SELECT
  subq_20.listings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_19.__listings AS listings
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      SUM(subq_18.__listings) AS __listings
    FROM (
      -- Pass Only Elements: ['__listings']
      SELECT
        subq_17.__listings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_16.listings AS __listings
          , subq_16.user__revenue_all_time
        FROM (
          -- Pass Only Elements: ['__listings', 'user__revenue_all_time']
          SELECT
            subq_15.user__revenue_all_time
            , subq_15.__listings AS listings
          FROM (
            -- Join Standard Outputs
            SELECT
              subq_14.user__revenue_all_time AS user__revenue_all_time
              , subq_6.ds__day AS ds__day
              , subq_6.ds__week AS ds__week
              , subq_6.ds__month AS ds__month
              , subq_6.ds__quarter AS ds__quarter
              , subq_6.ds__year AS ds__year
              , subq_6.ds__extract_year AS ds__extract_year
              , subq_6.ds__extract_quarter AS ds__extract_quarter
              , subq_6.ds__extract_month AS ds__extract_month
              , subq_6.ds__extract_day AS ds__extract_day
              , subq_6.ds__extract_dow AS ds__extract_dow
              , subq_6.ds__extract_doy AS ds__extract_doy
              , subq_6.created_at__day AS created_at__day
              , subq_6.created_at__week AS created_at__week
              , subq_6.created_at__month AS created_at__month
              , subq_6.created_at__quarter AS created_at__quarter
              , subq_6.created_at__year AS created_at__year
              , subq_6.created_at__extract_year AS created_at__extract_year
              , subq_6.created_at__extract_quarter AS created_at__extract_quarter
              , subq_6.created_at__extract_month AS created_at__extract_month
              , subq_6.created_at__extract_day AS created_at__extract_day
              , subq_6.created_at__extract_dow AS created_at__extract_dow
              , subq_6.created_at__extract_doy AS created_at__extract_doy
              , subq_6.listing__ds__day AS listing__ds__day
              , subq_6.listing__ds__week AS listing__ds__week
              , subq_6.listing__ds__month AS listing__ds__month
              , subq_6.listing__ds__quarter AS listing__ds__quarter
              , subq_6.listing__ds__year AS listing__ds__year
              , subq_6.listing__ds__extract_year AS listing__ds__extract_year
              , subq_6.listing__ds__extract_quarter AS listing__ds__extract_quarter
              , subq_6.listing__ds__extract_month AS listing__ds__extract_month
              , subq_6.listing__ds__extract_day AS listing__ds__extract_day
              , subq_6.listing__ds__extract_dow AS listing__ds__extract_dow
              , subq_6.listing__ds__extract_doy AS listing__ds__extract_doy
              , subq_6.listing__created_at__day AS listing__created_at__day
              , subq_6.listing__created_at__week AS listing__created_at__week
              , subq_6.listing__created_at__month AS listing__created_at__month
              , subq_6.listing__created_at__quarter AS listing__created_at__quarter
              , subq_6.listing__created_at__year AS listing__created_at__year
              , subq_6.listing__created_at__extract_year AS listing__created_at__extract_year
              , subq_6.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
              , subq_6.listing__created_at__extract_month AS listing__created_at__extract_month
              , subq_6.listing__created_at__extract_day AS listing__created_at__extract_day
              , subq_6.listing__created_at__extract_dow AS listing__created_at__extract_dow
              , subq_6.listing__created_at__extract_doy AS listing__created_at__extract_doy
              , subq_6.metric_time__day AS metric_time__day
              , subq_6.metric_time__week AS metric_time__week
              , subq_6.metric_time__month AS metric_time__month
              , subq_6.metric_time__quarter AS metric_time__quarter
              , subq_6.metric_time__year AS metric_time__year
              , subq_6.metric_time__extract_year AS metric_time__extract_year
              , subq_6.metric_time__extract_quarter AS metric_time__extract_quarter
              , subq_6.metric_time__extract_month AS metric_time__extract_month
              , subq_6.metric_time__extract_day AS metric_time__extract_day
              , subq_6.metric_time__extract_dow AS metric_time__extract_dow
              , subq_6.metric_time__extract_doy AS metric_time__extract_doy
              , subq_6.listing AS listing
              , subq_6.user AS user
              , subq_6.listing__user AS listing__user
              , subq_6.country_latest AS country_latest
              , subq_6.is_lux_latest AS is_lux_latest
              , subq_6.capacity_latest AS capacity_latest
              , subq_6.listing__country_latest AS listing__country_latest
              , subq_6.listing__is_lux_latest AS listing__is_lux_latest
              , subq_6.listing__capacity_latest AS listing__capacity_latest
              , subq_6.__listings AS __listings
              , subq_6.__lux_listings AS __lux_listings
              , subq_6.__smallest_listing AS __smallest_listing
              , subq_6.__largest_listing AS __largest_listing
              , subq_6.__active_listings AS __active_listings
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_5.ds__day
                , subq_5.ds__week
                , subq_5.ds__month
                , subq_5.ds__quarter
                , subq_5.ds__year
                , subq_5.ds__extract_year
                , subq_5.ds__extract_quarter
                , subq_5.ds__extract_month
                , subq_5.ds__extract_day
                , subq_5.ds__extract_dow
                , subq_5.ds__extract_doy
                , subq_5.created_at__day
                , subq_5.created_at__week
                , subq_5.created_at__month
                , subq_5.created_at__quarter
                , subq_5.created_at__year
                , subq_5.created_at__extract_year
                , subq_5.created_at__extract_quarter
                , subq_5.created_at__extract_month
                , subq_5.created_at__extract_day
                , subq_5.created_at__extract_dow
                , subq_5.created_at__extract_doy
                , subq_5.listing__ds__day
                , subq_5.listing__ds__week
                , subq_5.listing__ds__month
                , subq_5.listing__ds__quarter
                , subq_5.listing__ds__year
                , subq_5.listing__ds__extract_year
                , subq_5.listing__ds__extract_quarter
                , subq_5.listing__ds__extract_month
                , subq_5.listing__ds__extract_day
                , subq_5.listing__ds__extract_dow
                , subq_5.listing__ds__extract_doy
                , subq_5.listing__created_at__day
                , subq_5.listing__created_at__week
                , subq_5.listing__created_at__month
                , subq_5.listing__created_at__quarter
                , subq_5.listing__created_at__year
                , subq_5.listing__created_at__extract_year
                , subq_5.listing__created_at__extract_quarter
                , subq_5.listing__created_at__extract_month
                , subq_5.listing__created_at__extract_day
                , subq_5.listing__created_at__extract_dow
                , subq_5.listing__created_at__extract_doy
                , subq_5.ds__day AS metric_time__day
                , subq_5.ds__week AS metric_time__week
                , subq_5.ds__month AS metric_time__month
                , subq_5.ds__quarter AS metric_time__quarter
                , subq_5.ds__year AS metric_time__year
                , subq_5.ds__extract_year AS metric_time__extract_year
                , subq_5.ds__extract_quarter AS metric_time__extract_quarter
                , subq_5.ds__extract_month AS metric_time__extract_month
                , subq_5.ds__extract_day AS metric_time__extract_day
                , subq_5.ds__extract_dow AS metric_time__extract_dow
                , subq_5.ds__extract_doy AS metric_time__extract_doy
                , subq_5.listing
                , subq_5.user
                , subq_5.listing__user
                , subq_5.country_latest
                , subq_5.is_lux_latest
                , subq_5.capacity_latest
                , subq_5.listing__country_latest
                , subq_5.listing__is_lux_latest
                , subq_5.listing__capacity_latest
                , subq_5.__listings
                , subq_5.__lux_listings
                , subq_5.__smallest_listing
                , subq_5.__largest_listing
                , subq_5.__active_listings
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
              ) subq_5
            ) subq_6
            LEFT OUTER JOIN (
              -- Pass Only Elements: ['user', 'user__revenue_all_time']
              SELECT
                subq_13.user
                , subq_13.user__revenue_all_time
              FROM (
                -- Compute Metrics via Expressions
                SELECT
                  subq_12.user
                  , subq_12.revenue AS user__revenue_all_time
                FROM (
                  -- Compute Metrics via Expressions
                  SELECT
                    subq_11.user
                    , subq_11.__revenue AS revenue
                  FROM (
                    -- Aggregate Inputs for Simple Metrics
                    SELECT
                      subq_10.user
                      , SUM(subq_10.__revenue) AS __revenue
                    FROM (
                      -- Pass Only Elements: ['__revenue', 'user']
                      SELECT
                        subq_9.user
                        , subq_9.__revenue
                      FROM (
                        -- Pass Only Elements: ['__revenue', 'user']
                        SELECT
                          subq_8.user
                          , subq_8.__revenue
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
                            , subq_7.revenue_instance__ds__day
                            , subq_7.revenue_instance__ds__week
                            , subq_7.revenue_instance__ds__month
                            , subq_7.revenue_instance__ds__quarter
                            , subq_7.revenue_instance__ds__year
                            , subq_7.revenue_instance__ds__extract_year
                            , subq_7.revenue_instance__ds__extract_quarter
                            , subq_7.revenue_instance__ds__extract_month
                            , subq_7.revenue_instance__ds__extract_day
                            , subq_7.revenue_instance__ds__extract_dow
                            , subq_7.revenue_instance__ds__extract_doy
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
                            , subq_7.revenue_instance__user
                            , subq_7.__revenue
                          FROM (
                            -- Read Elements From Semantic Model 'revenue'
                            SELECT
                              revenue_src_28000.revenue AS __revenue
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
                          ) subq_7
                        ) subq_8
                      ) subq_9
                    ) subq_10
                    GROUP BY
                      subq_10.user
                  ) subq_11
                ) subq_12
              ) subq_13
            ) subq_14
            ON
              subq_6.user = subq_14.user
          ) subq_15
        ) subq_16
        WHERE user__revenue_all_time > 1
      ) subq_17
    ) subq_18
  ) subq_19
) subq_20
