-- Compute Metrics via Expressions
SELECT
  subq_3.metric_time__martian_day
  , subq_3.listing__ds__month
  , subq_3.listings
FROM (
  -- Aggregate Measures
  SELECT
    subq_2.metric_time__martian_day
    , subq_2.listing__ds__month
    , SUM(subq_2.listings) AS listings
  FROM (
    -- Join to Custom Granularity Dataset
    -- Pass Only Elements: ['listings', 'metric_time__martian_day', 'listing__ds__month']
    SELECT
      subq_0.listing__ds__month AS listing__ds__month
      , subq_0.listings AS listings
      , subq_1.martian_day AS metric_time__martian_day
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
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_1
    ON
      subq_0.ds__day = subq_1.ds
  ) subq_2
  GROUP BY
    subq_2.metric_time__martian_day
    , subq_2.listing__ds__month
) subq_3
