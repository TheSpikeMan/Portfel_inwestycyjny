-- Update statusów instrumentów --
-- Zapytanie aktualizuje kolumnę 'Status' W tabeli 'Instruments' --

UPDATE `projekt-inwestycyjny.Dane_instrumentow.Instruments`
SET Status = 1
WHERE
Ticker IN (
  SELECT
  DISTINCT Ticker FROM `projekt-inwestycyjny.Transactions.Present_transactions_view`
);

UPDATE `projekt-inwestycyjny.Dane_instrumentow.Instruments`
SET Status = 0
WHERE
Ticker NOT IN (
  SELECT
  DISTINCT Ticker FROM `projekt-inwestycyjny.Transactions.Present_transactions_view`
);



-- Konfiguracja
/*
Źródło: Scheduled Query:
Harmonogram (UTC): Every day at 22:10
Data rozpoczęcia: brak
Data zakończenia: brak
*/

