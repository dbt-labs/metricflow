test_name: test_metric_custom_granularity_joined_to_non_default_grain
test_filename: test_custom_granularity.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
SELECT
  nr_subq_3.metric_time__martian_day
  , nr_subq_3.listing__ds__month
  , nr_subq_3.listings
FROM (
  -- Aggregate Measures
  SELECT
    nr_subq_2.metric_time__martian_day
    , nr_subq_2.listing__ds__month
    , SUM(nr_subq_2.listings) AS listings
  FROM (
    -- Pass Only Elements: ['listings', 'metric_time__martian_day', 'listing__ds__month']
    SELECT
      nr_subq_1.metric_time__martian_day
      , nr_subq_1.listing__ds__month
      , nr_subq_1.listings
    FROM (
      -- Metric Time Dimension 'ds'
      -- Join to Custom Granularity Dataset
      SELECT
        nr_subq_28007.ds__day AS ds__day
        , nr_subq_28007.ds__week AS ds__week
        , nr_subq_28007.ds__month AS ds__month
        , nr_subq_28007.ds__quarter AS ds__quarter
        , nr_subq_28007.ds__year AS ds__year
        , nr_subq_28007.ds__extract_year AS ds__extract_year
        , nr_subq_28007.ds__extract_quarter AS ds__extract_quarter
        , nr_subq_28007.ds__extract_month AS ds__extract_month
        , nr_subq_28007.ds__extract_day AS ds__extract_day
        , nr_subq_28007.ds__extract_dow AS ds__extract_dow
        , nr_subq_28007.ds__extract_doy AS ds__extract_doy
        , nr_subq_28007.created_at__day AS created_at__day
        , nr_subq_28007.created_at__week AS created_at__week
        , nr_subq_28007.created_at__month AS created_at__month
        , nr_subq_28007.created_at__quarter AS created_at__quarter
        , nr_subq_28007.created_at__year AS created_at__year
        , nr_subq_28007.created_at__extract_year AS created_at__extract_year
        , nr_subq_28007.created_at__extract_quarter AS created_at__extract_quarter
        , nr_subq_28007.created_at__extract_month AS created_at__extract_month
        , nr_subq_28007.created_at__extract_day AS created_at__extract_day
        , nr_subq_28007.created_at__extract_dow AS created_at__extract_dow
        , nr_subq_28007.created_at__extract_doy AS created_at__extract_doy
        , nr_subq_28007.listing__ds__day AS listing__ds__day
        , nr_subq_28007.listing__ds__week AS listing__ds__week
        , nr_subq_28007.listing__ds__month AS listing__ds__month
        , nr_subq_28007.listing__ds__quarter AS listing__ds__quarter
        , nr_subq_28007.listing__ds__year AS listing__ds__year
        , nr_subq_28007.listing__ds__extract_year AS listing__ds__extract_year
        , nr_subq_28007.listing__ds__extract_quarter AS listing__ds__extract_quarter
        , nr_subq_28007.listing__ds__extract_month AS listing__ds__extract_month
        , nr_subq_28007.listing__ds__extract_day AS listing__ds__extract_day
        , nr_subq_28007.listing__ds__extract_dow AS listing__ds__extract_dow
        , nr_subq_28007.listing__ds__extract_doy AS listing__ds__extract_doy
        , nr_subq_28007.listing__created_at__day AS listing__created_at__day
        , nr_subq_28007.listing__created_at__week AS listing__created_at__week
        , nr_subq_28007.listing__created_at__month AS listing__created_at__month
        , nr_subq_28007.listing__created_at__quarter AS listing__created_at__quarter
        , nr_subq_28007.listing__created_at__year AS listing__created_at__year
        , nr_subq_28007.listing__created_at__extract_year AS listing__created_at__extract_year
        , nr_subq_28007.listing__created_at__extract_quarter AS listing__created_at__extract_quarter
        , nr_subq_28007.listing__created_at__extract_month AS listing__created_at__extract_month
        , nr_subq_28007.listing__created_at__extract_day AS listing__created_at__extract_day
        , nr_subq_28007.listing__created_at__extract_dow AS listing__created_at__extract_dow
        , nr_subq_28007.listing__created_at__extract_doy AS listing__created_at__extract_doy
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
        , nr_subq_28007.listing AS listing
        , nr_subq_28007.user AS user
        , nr_subq_28007.listing__user AS listing__user
        , nr_subq_28007.country_latest AS country_latest
        , nr_subq_28007.is_lux_latest AS is_lux_latest
        , nr_subq_28007.capacity_latest AS capacity_latest
        , nr_subq_28007.listing__country_latest AS listing__country_latest
        , nr_subq_28007.listing__is_lux_latest AS listing__is_lux_latest
        , nr_subq_28007.listing__capacity_latest AS listing__capacity_latest
        , nr_subq_28007.listings AS listings
        , nr_subq_28007.largest_listing AS largest_listing
        , nr_subq_28007.smallest_listing AS smallest_listing
        , nr_subq_0.martian_day AS metric_time__martian_day
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
      ) nr_subq_28007
      LEFT OUTER JOIN
        ***************************.mf_time_spine nr_subq_0
      ON
        nr_subq_28007.ds__day = nr_subq_0.ds
    ) nr_subq_1
  ) nr_subq_2
  GROUP BY
    nr_subq_2.metric_time__martian_day
    , nr_subq_2.listing__ds__month
) nr_subq_3
