SELECT
  metric_time__day
  , SUM(count_dogs) AS count_dogs
FROM (
  SELECT
    DATETIME_TRUNC(ds, day) AS metric_time__day
    , 1 AS count_dogs
  FROM ***************************.fct_animals animals_src_10000
) subq_2
GROUP BY
  metric_time__day
