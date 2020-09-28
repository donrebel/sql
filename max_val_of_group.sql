WITH
  d AS (
  SELECT
    *
  FROM
    UNNEST( 
    [STRUCT(1 AS id,'2020-01-01' AS s_date,100 AS amount), STRUCT(1 AS id,
        '2020-01-02' AS s_date,
        300 AS amount), STRUCT(1 AS id,
        '2020-01-03' AS s_date,
        200 AS amount), STRUCT(1 AS id,
        '2020-01-04' AS s_date,
        500 AS amount), STRUCT(1 AS id,
        '2020-01-05' AS s_date,
        700 AS amount), STRUCT(1 AS id,
        '2020-01-06' AS s_date,
        800 AS amount), STRUCT(1 AS id,
        '2020-01-07' AS s_date,
        100 AS amount), STRUCT(2 AS id,
        '2020-01-01' AS s_date,
        100 AS amount), STRUCT(3 AS id,
        '2020-01-02' AS s_date,
        300 AS amount), STRUCT(2 AS id,
        '2020-01-03' AS s_date,
        200 AS amount), STRUCT(2 AS id,
        '2020-01-04' AS s_date,
        500 AS amount), STRUCT(3 AS id,
        '2020-01-05' AS s_date,
        700 AS amount), STRUCT(3 AS id,
        '2020-01-06' AS s_date,
        800 AS amount), STRUCT(3 AS id,
        '2020-01-07' AS s_date,
        100 AS amount) ]) )
SELECT
  *
FROM (
  SELECT
    id,
    s_date,
    amount,
    CASE
      WHEN amount = MAX(amount) OVER (PARTITION BY id) THEN TRUE
  END
    AS is_max
  FROM
    d )
WHERE
  is_max
