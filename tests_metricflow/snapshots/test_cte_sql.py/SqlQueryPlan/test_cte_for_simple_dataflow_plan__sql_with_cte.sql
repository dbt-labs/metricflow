-- Read From CTE For node_id=rss_28001
-- Pass Only Elements: ['bookings',]
WITH rss_28001_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  SELECT
    1 AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
)

SELECT
  bookings AS bookings
FROM rss_28001_cte rss_28001_cte
