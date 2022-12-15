-- query
SELECT
  src2.bookings AS bookings
  , src3.listings AS listings
FROM (
  -- src4
  SELECT
    SUM(src4.listings) AS listings
  FROM demo.fct_listings src4
) src2
CROSS JOIN (
  -- src2
  SELECT
    SUM(src1.bookings) AS bookings
  FROM (
    -- src1
    SELECT
      1 AS bookings
    FROM demo.fct_bookings src0
  ) src1
) src3
