-- Test Percentile Expression
SELECT
  PERCENTILE_CONT(a.col0, 0.5) OVER() AS col0_percentile
FROM foo.bar a
