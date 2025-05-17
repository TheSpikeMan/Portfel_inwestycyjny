CREATE OR REPLACE TABLE `projekt-inwestycyjny.Dane_instrumentow.Daily`
(
  Ticker STRING(10)     NOT NULL OPTIONS(description="Ticker instrumentu finansowego"),
  Date  DATE NOT        NULL     OPTIONS(description="Data aktualizacji danych"),
  Close FLOAT64         NOT NULL OPTIONS(description="Kurs zamknięcia"),
  Volume INT64          NOT NULL OPTIONS(description="Dzienny wolumen"),
  Turnover INT64                 OPTIONS(description="Dzienny obrót")
);