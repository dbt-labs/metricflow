test_name: test_build_metric_tasks
test_filename: test_data_warehouse_tasks.py
sql_engine: Redshift
---
SELECT
  metric_time__day
  , SUM(__count_dogs) AS count_dogs
FROM (
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS __count_dogs
  FROM ***************************.fct_animals animals_src_10000
) subq_3
GROUP BY
  metric_time__day
