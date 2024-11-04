WITH
table1 AS (
  SELECT
    'Tomek'     AS name,
    '24'        AS age,
    'Murarz'    AS work

  UNION ALL
  SELECT
    'Adam'      AS name,
    '25'        AS age,
    'Kierowca'  AS work

  UNION ALL
  SELECT
    'Dawid'     AS name,
    '30'        AS age,
    'Atleta'    AS work
),

table2 AS (
  SELECT
    'Tomek'     AS name,
    '24'        AS age,
    'Murarz'    AS work

  UNION ALL
  SELECT
    'Krzysztof' AS name,
    '25'        AS age,
    'Kierowca'  AS work
),

table1_hash AS (
  SELECT
    *,
    FARM_FINGERPRINT(CONCAT(name, age, work)) AS hash1
  FROM table1
),

table2_hash AS (
  SELECT
    *,
    FARM_FINGERPRINT(CONCAT(name, age, work)) AS hash2
  FROM table2
)

SELECT
  *,
  'Row_only_in_table_1' AS source
FROM table1_hash
WHERE TRUE
  AND hash1 NOT IN (
    SELECT hash2
    FROM table2_hash
  )


UNION ALL
SELECT
  *,
  'Row_only_in_table_2' AS source
FROM table2_hash
WHERE TRUE
  AND hash2 NOT IN (
    SELECT hash1
    FROM table1_hash
  )
