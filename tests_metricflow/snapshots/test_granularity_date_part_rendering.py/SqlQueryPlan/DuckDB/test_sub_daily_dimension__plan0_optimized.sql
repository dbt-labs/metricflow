-- Read From SemanticModelDataSet('users_ds_source')
-- Pass Only Elements: ['user__bio_added_ts__second',]
SELECT
  DATE_TRUNC('second', bio_added_ts) AS user__bio_added_ts__second
FROM ***************************.dim_users users_ds_source_src_28000
GROUP BY
  DATE_TRUNC('second', bio_added_ts)
