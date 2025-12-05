test_name: test_multiple_categorical_dimension_pushdown
test_filename: test_predicate_pushdown_rendering.py
docstring:
  Tests rendering a query where we expect predicate pushdown for more than one categorical dimension.
sql_engine: DuckDB
---
-- Write to DataTable
SELECT
  subq_9.user__home_state_latest
  , subq_9.listings
FROM (
  -- Compute Metrics via Expressions
  SELECT
    subq_8.user__home_state_latest
    , subq_8.__listings AS listings
  FROM (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_7.user__home_state_latest
      , SUM(subq_7.__listings) AS __listings
    FROM (
      -- Pass Only Elements: ['__listings', 'user__home_state_latest']
      SELECT
        subq_6.user__home_state_latest
        , subq_6.__listings
      FROM (
        -- Constrain Output with WHERE
        SELECT
          subq_5.listings AS __listings
          , subq_5.listing__is_lux_latest
          , subq_5.listing__capacity_latest
          , subq_5.user__home_state_latest
        FROM (
          -- Pass Only Elements: ['__listings', 'user__home_state_latest', 'listing__is_lux_latest', 'listing__capacity_latest']
          SELECT
            subq_4.listing__is_lux_latest
            , subq_4.listing__capacity_latest
            , subq_4.user__home_state_latest
            , subq_4.__listings AS listings
          FROM (
            -- Join Standard Outputs
            SELECT
              subq_3.home_state_latest AS user__home_state_latest
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
              , subq_1.__listings AS __listings
              , subq_1.__lux_listings AS __lux_listings
              , subq_1.__smallest_listing AS __smallest_listing
              , subq_1.__largest_listing AS __largest_listing
              , subq_1.__active_listings AS __active_listings
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
                , subq_0.__listings
                , subq_0.__lux_listings
                , subq_0.__smallest_listing
                , subq_0.__largest_listing
                , subq_0.__active_listings
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
              ) subq_0
            ) subq_1
            LEFT OUTER JOIN (
              -- Pass Only Elements: ['home_state_latest', 'user']
              SELECT
                subq_2.user
                , subq_2.home_state_latest
              FROM (
                -- Read Elements From Semantic Model 'users_latest'
                SELECT
                  DATE_TRUNC('day', users_latest_src_28000.ds) AS ds_latest__day
                  , DATE_TRUNC('week', users_latest_src_28000.ds) AS ds_latest__week
                  , DATE_TRUNC('month', users_latest_src_28000.ds) AS ds_latest__month
                  , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS ds_latest__quarter
                  , DATE_TRUNC('year', users_latest_src_28000.ds) AS ds_latest__year
                  , EXTRACT(year FROM users_latest_src_28000.ds) AS ds_latest__extract_year
                  , EXTRACT(quarter FROM users_latest_src_28000.ds) AS ds_latest__extract_quarter
                  , EXTRACT(month FROM users_latest_src_28000.ds) AS ds_latest__extract_month
                  , EXTRACT(day FROM users_latest_src_28000.ds) AS ds_latest__extract_day
                  , EXTRACT(isodow FROM users_latest_src_28000.ds) AS ds_latest__extract_dow
                  , EXTRACT(doy FROM users_latest_src_28000.ds) AS ds_latest__extract_doy
                  , users_latest_src_28000.home_state_latest
                  , DATE_TRUNC('day', users_latest_src_28000.ds) AS user__ds_latest__day
                  , DATE_TRUNC('week', users_latest_src_28000.ds) AS user__ds_latest__week
                  , DATE_TRUNC('month', users_latest_src_28000.ds) AS user__ds_latest__month
                  , DATE_TRUNC('quarter', users_latest_src_28000.ds) AS user__ds_latest__quarter
                  , DATE_TRUNC('year', users_latest_src_28000.ds) AS user__ds_latest__year
                  , EXTRACT(year FROM users_latest_src_28000.ds) AS user__ds_latest__extract_year
                  , EXTRACT(quarter FROM users_latest_src_28000.ds) AS user__ds_latest__extract_quarter
                  , EXTRACT(month FROM users_latest_src_28000.ds) AS user__ds_latest__extract_month
                  , EXTRACT(day FROM users_latest_src_28000.ds) AS user__ds_latest__extract_day
                  , EXTRACT(isodow FROM users_latest_src_28000.ds) AS user__ds_latest__extract_dow
                  , EXTRACT(doy FROM users_latest_src_28000.ds) AS user__ds_latest__extract_doy
                  , users_latest_src_28000.home_state_latest AS user__home_state_latest
                  , users_latest_src_28000.user_id AS user
                FROM ***************************.dim_users_latest users_latest_src_28000
              ) subq_2
            ) subq_3
            ON
              subq_1.user = subq_3.user
          ) subq_4
        ) subq_5
        WHERE listing__is_lux_latest OR listing__capacity_latest > 4
      ) subq_6
    ) subq_7
    GROUP BY
      subq_7.user__home_state_latest
  ) subq_8
) subq_9
