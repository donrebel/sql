WITH
  d AS (
  SELECT
    *
  FROM
    UNNEST([ 
    STRUCT(62.71 AS num_var), 
    STRUCT(62.71 AS num_var), 
    STRUCT(62.71 AS num_var), 
    STRUCT(83.05 AS num_var), 
    STRUCT(99.66 AS num_var), 
    STRUCT(135.13 AS num_var), 
    STRUCT(135.13 AS num_var), 
    STRUCT(135.13 AS num_var), 
    STRUCT(198.20 AS num_var), 
    STRUCT(212.35 AS num_var), 
    STRUCT(302.44 AS num_var), 
    STRUCT(318.53 AS num_var),
    STRUCT(424.99 AS num_var), 
    STRUCT(22226.19 AS num_var) 
    ]))
SELECT
  num_var,
  SUM(num_var) OVER (ORDER BY num_var ROWS UNBOUNDED PRECEDING) AS cum_sum,
  SUM(num_var) OVER() AS sum_total,
  SUM(num_var) OVER (ORDER BY num_var ROWS UNBOUNDED PRECEDING) / SUM(num_var) OVER() AS cum_freq
FROM
  d
