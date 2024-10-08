-- Constrain Output with WHERE
-- Pass Only Elements: ['listing__ds__martian_day',]
SELECT
  listing__ds__martian_day
FROM (
  -- Read Elements From Semantic Model 'listings_latest'
  -- Join to Custom Granularity Dataset
  SELECT
    subq_3.martian_day AS listing__ds__martian_day
  FROM ***************************.dim_listings_latest listings_latest_src_28000
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_3
  ON
    DATETIME_TRUNC(listings_latest_src_28000.created_at, day) = subq_3.ds
) subq_4
WHERE listing__ds__martian_day = '2020-01-01'
GROUP BY
  listing__ds__martian_day
