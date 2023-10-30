-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  date_day,
  COALESCE(count_dogs, 0)
FROM
(
  SELECT
    metric_time__day
    , SUM(count_dogs) AS count_dogs
  FROM (
    -- Read Elements From Semantic Model 'animals'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['count_dogs', 'metric_time__day']
    SELECT
      DATE_TRUNC(ds, day) AS metric_time__day
      , 1 AS count_dogs
    FROM ***************************.fct_animals animals_src_0
  ) subq_2
  GROUP BY
    metric_time__day
) a
OUTER JOIN mf_time_spine b on DATE_TRUNC(date_day, day) = metric_time__day
  AND DATE_TRUNC(date_day, day) = date_day
