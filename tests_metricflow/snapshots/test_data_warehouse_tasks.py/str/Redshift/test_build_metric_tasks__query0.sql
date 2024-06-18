SELECT
  metric_time__day
  , SUM(count_dogs) AS count_dogs
FROM (
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , 1 AS count_dogs
  FROM ***************************.fct_animals animals_src_10000
) subq_2
GROUP BY
  metric_time__day
