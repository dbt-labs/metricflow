-- Combine Metrics
-- Compute Metrics via Expressions
SELECT
  COALESCE(subq_30.ds__day, subq_40.ds__day) AS ds__day
  , COALESCE(subq_30.listing__country_latest, subq_40.listing__country_latest) AS listing__country_latest
  , CAST(subq_30.bookings AS DOUBLE) / CAST(NULLIF(subq_40.views, 0) AS DOUBLE) AS bookings_per_view
FROM (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['bookings', 'listing__country_latest', 'ds__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_23.ds__day AS ds__day
    , listings_latest_src_10004.country AS listing__country_latest
    , SUM(subq_23.bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'ds__day', 'listing']
    SELECT
      DATE_TRUNC('day', ds) AS ds__day
      , listing_id AS listing
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_23
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10004
  ON
    subq_23.listing = listings_latest_src_10004.listing_id
  GROUP BY
    subq_23.ds__day
    , listings_latest_src_10004.country
) subq_30
INNER JOIN (
  -- Join Standard Outputs
  -- Pass Only Elements:
  --   ['views', 'listing__country_latest', 'ds__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_33.ds__day AS ds__day
    , listings_latest_src_10004.country AS listing__country_latest
    , SUM(subq_33.views) AS views
  FROM (
    -- Read Elements From Semantic Model 'views_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['views', 'ds__day', 'listing']
    SELECT
      DATE_TRUNC('day', ds) AS ds__day
      , listing_id AS listing
      , 1 AS views
    FROM ***************************.fct_views views_source_src_10009
  ) subq_33
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10004
  ON
    subq_33.listing = listings_latest_src_10004.listing_id
  GROUP BY
    subq_33.ds__day
    , listings_latest_src_10004.country
) subq_40
ON
  (
    (
      subq_30.listing__country_latest = subq_40.listing__country_latest
    ) OR (
      (
        subq_30.listing__country_latest IS NULL
      ) AND (
        subq_40.listing__country_latest IS NULL
      )
    )
  ) AND (
    (
      subq_30.ds__day = subq_40.ds__day
    ) OR (
      (subq_30.ds__day IS NULL) AND (subq_40.ds__day IS NULL)
    )
  )
