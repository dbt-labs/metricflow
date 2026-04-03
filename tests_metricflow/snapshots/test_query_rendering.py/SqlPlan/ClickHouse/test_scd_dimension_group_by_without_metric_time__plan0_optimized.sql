test_name: test_scd_dimension_group_by_without_metric_time
test_filename: test_query_rendering.py
sql_engine: ClickHouse
---
SELECT
  listing__capacity
  , SUM(__family_bookings) AS family_bookings
FROM (
  SELECT
    listing__capacity
    , family_bookings AS __family_bookings
  FROM (
    SELECT
      listings_src_26000.capacity AS listing__capacity
      , subq_11.__family_bookings AS family_bookings
    FROM (
      SELECT
        toStartOfDay(ds) AS metric_time__day
        , listing_id AS listing
        , 1 AS __family_bookings
      FROM ***************************.fct_bookings bookings_source_src_26000
    ) subq_11
    LEFT OUTER JOIN
      ***************************.dim_listings listings_src_26000
    ON
      (
        subq_11.listing = listings_src_26000.listing_id
      ) AND (
        (
          subq_11.metric_time__day >= listings_src_26000.active_from
        ) AND (
          (
            subq_11.metric_time__day < listings_src_26000.active_to
          ) OR (
            listings_src_26000.active_to IS NULL
          )
        )
      )
  ) subq_15
  WHERE listing__capacity > 2
) subq_17
GROUP BY
  listing__capacity
