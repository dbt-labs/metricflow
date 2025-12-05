test_name: test_sub_daily_dimension
test_filename: test_granularity_date_part_rendering.py
sql_engine: BigQuery
---
-- Read Elements From Semantic Model 'users_ds_source'
-- Pass Only Elements: ['user__bio_added_ts__second']
-- Pass Only Elements: ['user__bio_added_ts__second']
-- Write to DataTable
SELECT
  DATETIME_TRUNC(bio_added_ts, second) AS user__bio_added_ts__second
FROM ***************************.dim_users users_ds_source_src_28000
GROUP BY
  user__bio_added_ts__second
