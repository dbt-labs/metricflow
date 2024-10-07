-- Constrain Output with WHERE
-- Pass Only Elements: ['listing__ds__martian_day',]
SELECT
  listing__ds__martian_day
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Join to Custom Granularity Dataset
  SELECT
    subq_2.martian_day AS listing__ds__martian_day
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_2
  ON
    DATE_TRUNC('day', listings_latest_src_28000.created_at) = subq_2.ds
) subq_3
WHERE listing__ds__martian_day = '2020-01-01'
GROUP BY
  listing__ds__martian_day
