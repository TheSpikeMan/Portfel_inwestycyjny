-- Utworzenie datasetów

CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Calendar`;
CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Dane_instrumentow`;
CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Inflation`;
CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Transactions`;
CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Waluty`;

-- UTWORZENIE TABEL W ZBIORZE CALENDAR --
-- Tabela przechowująca przypisanie kwartału do określonej daty --
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Calendar.Dates` (
  `Date` DATE,
  Quarter STRING
);

-- UTWORZENIE TABEL W ZBIORZE DANE_INSTRUMENTÓW -- 
-- Tabela przechowująca dane giełdowe instrumentów -- 
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Dane_instrumentow.Daily`(
  Ticker STRING NOT NULL,
  `Date` DATE NOT NULL,
  Close FLOAT64 NOT NULL,
  Volume INT64 NOT NULL,
  Turnover INT64 NOT NULL
);

-- Tabela przechowująca unikatowe instrumenty --
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Dane_instrumentow.Instruments`(
  Instrument_id INT64 NOT NULL,
  Ticker STRING NOT NULL,
  Name STRING NOT NULL,
  Unit INT64 NOT NULL,
  Market STRING NOT NULL,
  Distribution_policy STRING NOT NULL,
  Instrument_type_id INT64 NOT NULL,
  Instrument_headquarter STRING NOT NULL,
  Status INT64 NOT NULL
);

-- Tabela przechowująca typy instrumentów -- 
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Dane_instrumentow.Instruments_types` (
  Instrument_type_id INT64 NOT NULL,
  Instrument_type STRING NOT NULL
);

-- Tabela przechowująca Tickery wykorzystane do scrapowania - do przerobienia --
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Dane_instrumentow.Tickers`(
  Ticker STRING NOT NULL,
  Status BOOL NOT NULL
);

-- W zbiorze znajduje się również widok 'Trends_analysis'


-- UTWORZENIE TABEL W ZBIORZE INFLATION -- 
-- Tabela Inflation zawiera dane inflacyjne scrapowane co miesiąc --
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Inflation.Inflation`(
  `date` STRING NOT NULL,
  inflation FLOAT NOT NULL
);

-- UTWORZENIE TABEL W ZBIORZE TRANSACTIONS --
-- Tabela Transactions zawiera dane transakcyjne --
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Transactions.Transactions`(
  Transaction_id INT64 NOT NULL,
  Transaction_date DATE NOT NULL,
  Transaction_type STRING NOT NULL,
  Currency STRING NOT NULL,
  Transaction_price FLOAT64 NOT NULL,
  Transaction_amount FLOAT64 NOT NULL,
  Instrument_id INT64 NOT NULL,
  Commision FLOAT64,
  Dirty_bond_price FLOAT64
);

/*
W zbiorze znajduję się jeszcze następujące widoki:
- Dividend_view,
- Finished_transactions_view,
- Present_transactions_view,
- Transactions_view
*/

-- UTWORZENIE TABEL W ZBIORZE WALUTY -- 
-- Tabela Currency zawiera dane walutowe --
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Waluty.Currency`(
  Currency_date DATE NOT NULL,
  Currency STRING NOT NULL,
  Currency_close FLOAT NOT NULL,
);


