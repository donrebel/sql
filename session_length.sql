WITH
  d AS (
  SELECT
    *
  FROM
    UNNEST([ STRUCT(1 AS id,
        '2020-01-01' AS s_date), STRUCT(1 AS id,
        '2020-01-02' AS s_date), STRUCT(1 AS id,
        '2020-01-03' AS s_date), STRUCT(1 AS id,
        '2020-01-10' AS s_date), STRUCT(1 AS id,
        '2020-01-11' AS s_date), STRUCT(2 AS id,
        '2020-01-01' AS s_date), STRUCT(2 AS id,
        '2020-01-02' AS s_date), STRUCT(2 AS id,
        '2020-01-03' AS s_date), STRUCT(2 AS id,
        '2020-01-11' AS s_date), STRUCT(2 AS id,
        '2020-01-12' AS s_date), STRUCT(2 AS id,
        '2020-01-13' AS s_date), STRUCT(2 AS id,
        '2020-01-14' AS s_date), STRUCT(2 AS id,
        '2020-01-15' AS s_date) ]))
SELECT
  id,
  MIN(d_date) AS session_start,
  MAX(d_date) AS session_end,
  COUNT(1) AS session_length
FROM (
  SELECT
    id,
    d_date,
    DATE_ADD(d_date, INTERVAL -1*rn DAY) AS sgrp
  FROM (
    SELECT
      id,
      parse_DATE('%F',
        s_date) AS d_date,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY s_date) AS rn
    FROM
      d ) )
GROUP BY
  id,
  sgrp
