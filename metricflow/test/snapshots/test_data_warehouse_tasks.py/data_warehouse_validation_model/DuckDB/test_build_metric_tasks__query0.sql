-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time
  , SUM(count_dogs) AS count_dogs
FROM (
  -- Read Elements From Semantic Model 'animals'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['count_dogs', 'metric_time']
  SELECT
    ds AS metric_time
    , 1 AS count_dogs
  FROM ***************************.fct_animals animals_src_0
) subq_2
GROUP BY
  metric_time
