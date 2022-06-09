-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(count_dogs) AS count_dogs
  , ds
FROM (
  -- Read Elements From Data Source 'animals'
  -- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
  -- Pass Only Elements:
  --   ['count_dogs', 'ds']
  SELECT
    1 AS count_dogs
    , ds
  FROM (
    SELECT * FROM ***************************.fct_animals
  ) animals_src_0
  WHERE (
    ds >= CAST('2000-01-01' AS TIMESTAMP)
  ) AND (
    ds <= CAST('2040-12-31' AS TIMESTAMP)
  )
) subq_2
GROUP BY
  ds
