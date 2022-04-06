-- src1
-- src2
-- src3
SELECT
  SUM(src0.bookings) AS bookings
  , src0.ds
FROM demo.fct_bookings src0
WHERE (src2.ds <= '2020-01-05') AND (src0.ds >= '2020-01-01')
GROUP BY
  src0.ds
ORDER BY ds
LIMIT 1
