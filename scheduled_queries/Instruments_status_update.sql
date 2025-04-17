-- Update statusów instrumentów --
-- Zapytanie aktualizuje kolumnę 'Status' W tabeli 'Instruments' --

UPDATE `projekt-inwestycyjny.Dane_instrumentow.Instruments` AS instruments
SET Status = 1
WHERE EXISTS
(
    SELECT 1
    FROM `projekt-inwestycyjny.Transactions.Present_transactions_view` AS present_transactions
    WHERE TRUE
      AND instruments.ticker = present_transactions.Ticker
      AND instruments.project_id = present_transactions.Project_id
  );

UPDATE `projekt-inwestycyjny.Dane_instrumentow.Instruments` AS instruments
SET Status = 0
WHERE NOT EXISTS
(
    SELECT 1
    FROM `projekt-inwestycyjny.Transactions.Present_transactions_view` AS present_transactions
    WHERE TRUE
      AND instruments.ticker = present_transactions.Ticker
      AND instruments.project_id = present_transactions.Project_id
  );



-- Konfiguracja
/*
Źródło: Scheduled Query:
Harmonogram (UTC): Every day at 22:10
Data rozpoczęcia: brak
Data zakończenia: brak
*/

