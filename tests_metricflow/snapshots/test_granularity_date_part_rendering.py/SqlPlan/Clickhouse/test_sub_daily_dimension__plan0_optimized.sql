test_name: test_sub_daily_dimension
test_filename: test_granularity_date_part_rendering.py
sql_engine: Clickhouse
---
-- Read Elements From Semantic Model 'users_ds_source'
-- Pass Only Elements: ['user__bio_added_ts__second',]
SELECT
  DATE_TRUNC('second', bio_added_ts) AS user__bio_added_ts__second
FROM ***************************.dim_users users_ds_source_src_28000
GROUP BY
  DATE_TRUNC('second', bio_added_ts)
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
