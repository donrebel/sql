WITH
  d AS (
  SELECT
    *
  FROM
    UNNEST( [ STRUCT(1 AS subscr_id,
        'active' AS status,
        '2020-01-01' AS status_date), STRUCT(1 AS subscr_id,
        'cancelled' AS status,
        '2020-01-03' AS status_date), STRUCT(1 AS subscr_id,
        'active' AS status,
        '2020-01-07' AS status_date), STRUCT(1 AS subscr_id,
        'cancelled' AS status,
        '2020-01-15' AS status_date), STRUCT(2 AS subscr_id,
        'active' AS status,
        '2020-01-01' AS status_date), STRUCT(2 AS subscr_id,
        'cancelled' AS status,
        '2020-01-10' AS status_date), STRUCT(3 AS subscr_id,
        'active' AS status,
        '2020-01-04' AS status_date), STRUCT(4 AS subscr_id,
        'pending' AS status,
        '2020-01-04' AS status_date), STRUCT(4 AS subscr_id,
        'active' AS status,
        '2020-01-06' AS status_date), STRUCT(5 AS subscr_id,
        'active' AS status,
        '2020-01-03' AS status_date) ] ) ),
  calendar AS (
  SELECT
    FORMAT_DATE('%F', calendar_date) AS calendar_date
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2020-01-25', INTERVAL 1 DAY)) AS calendar_date )
SELECT
  calendar_date,
  COUNT(subscr_id),
  ARRAY_AGG(subscr_id)
FROM
  calendar AS c
LEFT JOIN (
  SELECT
    subscr_id,
    status,
    status_date AS period_start,
    ifnull(LAG(status_date) OVER (PARTITION BY subscr_id ORDER BY status_date DESC),
      '9999-12-31') AS period_end
  FROM
    d ) AS s
ON
  c.calendar_date BETWEEN s.period_start
  AND s.period_end
WHERE
  status = 'active'
GROUP BY
  1
ORDER BY
  1
