BEGIN
  CREATE TEMPORARY TABLE testing_set AS
  WITH test_data AS (
      SELECT 1 AS report_number, 'Szczecin' AS miasto
      UNION ALL
      SELECT 2 AS report_number, 'Poznań' AS miasto
      UNION ALL
      SELECT 3 AS report_number, 'Wrocław' AS miasto
      UNION ALL
      SELECT 4 AS report_number, 'Gdańsk' AS miasto
  )

  SELECT * FROM test_data;

  FOR number IN (
    SELECT DISTINCT
      report_number
    FROM testing_set
  ) DO
      SELECT *
      FROM testing_set
      WHERE report_number = number.report_number;
  END FOR;
END;