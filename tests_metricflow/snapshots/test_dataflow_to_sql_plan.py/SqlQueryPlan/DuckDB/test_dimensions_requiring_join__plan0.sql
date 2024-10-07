-- Join Standard Outputs
-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest']
SELECT
  subq_0.listing__is_lux_latest AS listing__is_lux_latest
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
FULL OUTER JOIN (
  -- Read From SemanticModelDataSet('users_latest')
  -- Pass Only Elements: ['home_state_latest', 'user']
  SELECT
    users_latest_src_28000.home_state_latest
    , users_latest_src_28000.user_id AS user
  FROM ***************************.dim_users_latest users_latest_src_28000
) subq_1
ON
  subq_0.user = subq_1.user
GROUP BY
  subq_0.listing__is_lux_latest
