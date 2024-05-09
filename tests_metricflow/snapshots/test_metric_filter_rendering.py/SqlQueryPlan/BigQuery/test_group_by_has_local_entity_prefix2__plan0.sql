-- Compute Metrics via Expressions
SELECT
  subq_17.listings
FROM (
  -- Aggregate Measures
  SELECT
    SUM(subq_16.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings',]
    SELECT
      subq_15.listings
    FROM (
      -- Constrain Output with WHERE
      SELECT
        subq_14.listing__view__listing__views
        , subq_14.listings
      FROM (
        -- Pass Only Elements: ['listings', 'listing__view__listing__views']
        SELECT
          subq_13.listing__view__listing__views
          , subq_13.listings
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_6.listing AS listing
            , subq_12.view__listing AS listing__view__listing
            , subq_12.view__listing__views AS listing__view__listing__views
            , subq_6.listings AS listings
          FROM (
            -- Pass Only Elements: ['listings', 'listing']
            SELECT
              subq_5.listing
              , subq_5.listings
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
                , subq_4.listings
                , subq_4.largest_listing
                , subq_4.smallest_listing
              FROM (
                -- Read Elements From Semantic Model 'listings_latest'
                SELECT
                  1 AS listings
                  , listings_latest_src_28000.capacity AS largest_listing
                  , listings_latest_src_28000.capacity AS smallest_listing
                  , DATE_TRUNC(listings_latest_src_28000.created_at, day) AS ds__day
                  , DATE_TRUNC(listings_latest_src_28000.created_at, isoweek) AS ds__week
                  , DATE_TRUNC(listings_latest_src_28000.created_at, month) AS ds__month
                  , DATE_TRUNC(listings_latest_src_28000.created_at, quarter) AS ds__quarter
                  , DATE_TRUNC(listings_latest_src_28000.created_at, year) AS ds__year
                  , EXTRACT(year FROM listings_latest_src_28000.created_at) AS ds__extract_year
                  , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS ds__extract_quarter
                  , EXTRACT(month FROM listings_latest_src_28000.created_at) AS ds__extract_month
                  , EXTRACT(day FROM listings_latest_src_28000.created_at) AS ds__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS ds__extract_dow
                  , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS ds__extract_doy
                  , DATE_TRUNC(listings_latest_src_28000.created_at, day) AS created_at__day
                  , DATE_TRUNC(listings_latest_src_28000.created_at, isoweek) AS created_at__week
                  , DATE_TRUNC(listings_latest_src_28000.created_at, month) AS created_at__month
                  , DATE_TRUNC(listings_latest_src_28000.created_at, quarter) AS created_at__quarter
                  , DATE_TRUNC(listings_latest_src_28000.created_at, year) AS created_at__year
                  , EXTRACT(year FROM listings_latest_src_28000.created_at) AS created_at__extract_year
                  , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS created_at__extract_quarter
                  , EXTRACT(month FROM listings_latest_src_28000.created_at) AS created_at__extract_month
                  , EXTRACT(day FROM listings_latest_src_28000.created_at) AS created_at__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS created_at__extract_dow
                  , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS created_at__extract_doy
                  , listings_latest_src_28000.country AS country_latest
                  , listings_latest_src_28000.is_lux AS is_lux_latest
                  , listings_latest_src_28000.capacity AS capacity_latest
                  , DATE_TRUNC(listings_latest_src_28000.created_at, day) AS listing__ds__day
                  , DATE_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__ds__week
                  , DATE_TRUNC(listings_latest_src_28000.created_at, month) AS listing__ds__month
                  , DATE_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__ds__quarter
                  , DATE_TRUNC(listings_latest_src_28000.created_at, year) AS listing__ds__year
                  , EXTRACT(year FROM listings_latest_src_28000.created_at) AS listing__ds__extract_year
                  , EXTRACT(quarter FROM listings_latest_src_28000.created_at) AS listing__ds__extract_quarter
                  , EXTRACT(month FROM listings_latest_src_28000.created_at) AS listing__ds__extract_month
                  , EXTRACT(day FROM listings_latest_src_28000.created_at) AS listing__ds__extract_day
                  , IF(EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) = 1, 7, EXTRACT(dayofweek FROM listings_latest_src_28000.created_at) - 1) AS listing__ds__extract_dow
                  , EXTRACT(dayofyear FROM listings_latest_src_28000.created_at) AS listing__ds__extract_doy
                  , DATE_TRUNC(listings_latest_src_28000.created_at, day) AS listing__created_at__day
                  , DATE_TRUNC(listings_latest_src_28000.created_at, isoweek) AS listing__created_at__week
                  , DATE_TRUNC(listings_latest_src_28000.created_at, month) AS listing__created_at__month
                  , DATE_TRUNC(listings_latest_src_28000.created_at, quarter) AS listing__created_at__quarter
                  , DATE_TRUNC(listings_latest_src_28000.created_at, year) AS listing__created_at__year
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
              ) subq_4
            ) subq_5
          ) subq_6
          LEFT OUTER JOIN (
            -- Pass Only Elements: ['view__listing', 'view__listing__views']
            SELECT
              subq_11.view__listing
              , subq_11.view__listing__views
            FROM (
              -- Compute Metrics via Expressions
              SELECT
                subq_10.view__listing
                , subq_10.views AS view__listing__views
              FROM (
                -- Aggregate Measures
                SELECT
                  subq_9.view__listing
                  , SUM(subq_9.views) AS views
                FROM (
                  -- Pass Only Elements: ['views', 'view__listing']
                  SELECT
                    subq_8.view__listing
                    , subq_8.views
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
                      , subq_7.ds_partitioned__day
                      , subq_7.ds_partitioned__week
                      , subq_7.ds_partitioned__month
                      , subq_7.ds_partitioned__quarter
                      , subq_7.ds_partitioned__year
                      , subq_7.ds_partitioned__extract_year
                      , subq_7.ds_partitioned__extract_quarter
                      , subq_7.ds_partitioned__extract_month
                      , subq_7.ds_partitioned__extract_day
                      , subq_7.ds_partitioned__extract_dow
                      , subq_7.ds_partitioned__extract_doy
                      , subq_7.view__ds__day
                      , subq_7.view__ds__week
                      , subq_7.view__ds__month
                      , subq_7.view__ds__quarter
                      , subq_7.view__ds__year
                      , subq_7.view__ds__extract_year
                      , subq_7.view__ds__extract_quarter
                      , subq_7.view__ds__extract_month
                      , subq_7.view__ds__extract_day
                      , subq_7.view__ds__extract_dow
                      , subq_7.view__ds__extract_doy
                      , subq_7.view__ds_partitioned__day
                      , subq_7.view__ds_partitioned__week
                      , subq_7.view__ds_partitioned__month
                      , subq_7.view__ds_partitioned__quarter
                      , subq_7.view__ds_partitioned__year
                      , subq_7.view__ds_partitioned__extract_year
                      , subq_7.view__ds_partitioned__extract_quarter
                      , subq_7.view__ds_partitioned__extract_month
                      , subq_7.view__ds_partitioned__extract_day
                      , subq_7.view__ds_partitioned__extract_dow
                      , subq_7.view__ds_partitioned__extract_doy
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
                      , subq_7.listing
                      , subq_7.user
                      , subq_7.view__listing
                      , subq_7.view__user
                      , subq_7.views
                    FROM (
                      -- Read Elements From Semantic Model 'views_source'
                      SELECT
                        1 AS views
                        , DATE_TRUNC(views_source_src_28000.ds, day) AS ds__day
                        , DATE_TRUNC(views_source_src_28000.ds, isoweek) AS ds__week
                        , DATE_TRUNC(views_source_src_28000.ds, month) AS ds__month
                        , DATE_TRUNC(views_source_src_28000.ds, quarter) AS ds__quarter
                        , DATE_TRUNC(views_source_src_28000.ds, year) AS ds__year
                        , EXTRACT(year FROM views_source_src_28000.ds) AS ds__extract_year
                        , EXTRACT(quarter FROM views_source_src_28000.ds) AS ds__extract_quarter
                        , EXTRACT(month FROM views_source_src_28000.ds) AS ds__extract_month
                        , EXTRACT(day FROM views_source_src_28000.ds) AS ds__extract_day
                        , IF(EXTRACT(dayofweek FROM views_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM views_source_src_28000.ds) - 1) AS ds__extract_dow
                        , EXTRACT(dayofyear FROM views_source_src_28000.ds) AS ds__extract_doy
                        , DATE_TRUNC(views_source_src_28000.ds_partitioned, day) AS ds_partitioned__day
                        , DATE_TRUNC(views_source_src_28000.ds_partitioned, isoweek) AS ds_partitioned__week
                        , DATE_TRUNC(views_source_src_28000.ds_partitioned, month) AS ds_partitioned__month
                        , DATE_TRUNC(views_source_src_28000.ds_partitioned, quarter) AS ds_partitioned__quarter
                        , DATE_TRUNC(views_source_src_28000.ds_partitioned, year) AS ds_partitioned__year
                        , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_year
                        , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_quarter
                        , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_month
                        , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_day
                        , IF(EXTRACT(dayofweek FROM views_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM views_source_src_28000.ds_partitioned) - 1) AS ds_partitioned__extract_dow
                        , EXTRACT(dayofyear FROM views_source_src_28000.ds_partitioned) AS ds_partitioned__extract_doy
                        , DATE_TRUNC(views_source_src_28000.ds, day) AS view__ds__day
                        , DATE_TRUNC(views_source_src_28000.ds, isoweek) AS view__ds__week
                        , DATE_TRUNC(views_source_src_28000.ds, month) AS view__ds__month
                        , DATE_TRUNC(views_source_src_28000.ds, quarter) AS view__ds__quarter
                        , DATE_TRUNC(views_source_src_28000.ds, year) AS view__ds__year
                        , EXTRACT(year FROM views_source_src_28000.ds) AS view__ds__extract_year
                        , EXTRACT(quarter FROM views_source_src_28000.ds) AS view__ds__extract_quarter
                        , EXTRACT(month FROM views_source_src_28000.ds) AS view__ds__extract_month
                        , EXTRACT(day FROM views_source_src_28000.ds) AS view__ds__extract_day
                        , IF(EXTRACT(dayofweek FROM views_source_src_28000.ds) = 1, 7, EXTRACT(dayofweek FROM views_source_src_28000.ds) - 1) AS view__ds__extract_dow
                        , EXTRACT(dayofyear FROM views_source_src_28000.ds) AS view__ds__extract_doy
                        , DATE_TRUNC(views_source_src_28000.ds_partitioned, day) AS view__ds_partitioned__day
                        , DATE_TRUNC(views_source_src_28000.ds_partitioned, isoweek) AS view__ds_partitioned__week
                        , DATE_TRUNC(views_source_src_28000.ds_partitioned, month) AS view__ds_partitioned__month
                        , DATE_TRUNC(views_source_src_28000.ds_partitioned, quarter) AS view__ds_partitioned__quarter
                        , DATE_TRUNC(views_source_src_28000.ds_partitioned, year) AS view__ds_partitioned__year
                        , EXTRACT(year FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_year
                        , EXTRACT(quarter FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_quarter
                        , EXTRACT(month FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_month
                        , EXTRACT(day FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_day
                        , IF(EXTRACT(dayofweek FROM views_source_src_28000.ds_partitioned) = 1, 7, EXTRACT(dayofweek FROM views_source_src_28000.ds_partitioned) - 1) AS view__ds_partitioned__extract_dow
                        , EXTRACT(dayofyear FROM views_source_src_28000.ds_partitioned) AS view__ds_partitioned__extract_doy
                        , views_source_src_28000.listing_id AS listing
                        , views_source_src_28000.user_id AS user
                        , views_source_src_28000.listing_id AS view__listing
                        , views_source_src_28000.user_id AS view__user
                      FROM ***************************.fct_views views_source_src_28000
                    ) subq_7
                  ) subq_8
                ) subq_9
                GROUP BY
                  view__listing
              ) subq_10
            ) subq_11
          ) subq_12
          ON
            subq_6.listing = subq_12.view__listing
        ) subq_13
      ) subq_14
      WHERE listing__view__listing__views > 2
    ) subq_15
  ) subq_16
) subq_17
