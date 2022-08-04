-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(count_dogs) AS count_dogs
  , metric_time
FROM (
  -- Read Elements From Data Source 'animals'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
  -- Pass Only Elements:
  --   ['count_dogs', 'metric_time']
  SELECT
    1 AS count_dogs
    , ds AS metric_time
  FROM (
    SELECT * FROM ***************************.fct_animals
  ) animals_src_0
  WHERE (
    ds >= CAST('2000-01-01' AS TEXT)
  ) AND (
    ds <= CAST('2040-12-31' AS TEXT)
  )
) subq_3
GROUP BY
  metric_time
