-- Read Elements From Semantic Model 'users_ds_source'
-- Pass Only Elements: ['user__bio_added_ts__second',]
SELECT
  DATETIME_TRUNC(bio_added_ts, second) AS user__bio_added_ts__second
FROM ***************************.dim_users users_ds_source_src_28000
GROUP BY
  user__bio_added_ts__second
