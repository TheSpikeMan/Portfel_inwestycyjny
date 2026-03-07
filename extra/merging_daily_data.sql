MERGE `projekt-inwestycyjny.Dane_instrumentow.Daily` AS target
USING `projekt-inwestycyjny.Analysis.Compare table` AS source
ON target.Date = source.Date
AND target.Ticker = source.Ticker

-- if date and ticker found, update Close
WHEN MATCHED THEN
  UPDATE SET
    target.Close = source.Close_Yahoo_Finance

-- If date and ticker not found insert new rows
WHEN NOT MATCHED THEN
  INSERT (Project_id, Ticker, Date, Close, Volume, Turnover)
  VALUES (null, source.Ticker, source.Date, source.Close_Yahoo_Finance, null, null);