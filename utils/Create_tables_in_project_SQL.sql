-- Utworzenie datasetów

CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Calendar`;
CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Dane_instrumentow`;
CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Inflation`;
CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Transactions`;
CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Waluty`;

-- UTWORZENIE TABEL W ZBIORZE CALENDAR --
-- Tabela przechowująca przypisanie kwartału do określonej daty --
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Calendar.Dates` (
  `Date`  DATE      OPTIONS(description="Data"),
  Quarter STRING    OPTIONS(description="Kwartał")
);

-- Uzupełnienie tabeli Dates --

SELECT
  *,
  CASE
    WHEN EXTRACT(MONTH FROM `Date`) IN (1, 2, 3)    THEN 'I kwartał'
    WHEN EXTRACT(MONTH FROM `Date`) IN (4, 5, 6)    THEN 'II kwartał'
    WHEN EXTRACT(MONTH FROM `Date`) IN (7, 8, 9)    THEN 'III kwartał'
    WHEN EXTRACT(MONTH FROM `Date`) IN (10, 11, 12) THEN 'IV kwartał'
  END AS Quarter
FROM
UNNEST(
  GENERATE_DATE_ARRAY("2020-01-01", "2030-01-01", INTERVAL 1 DAY)) as `Date`;


-- UTWORZENIE TABEL W ZBIORZE DANE_INSTRUMENTÓW -- 
-- Tabela przechowująca dane giełdowe instrumentów -- 
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Dane_instrumentow.Daily`(
  Ticker   STRING   NOT NULL OPTIONS(description="Ticker instrumentu finansowego"),
  `Date`   DATE     NOT NULL OPTIONS(description="Data aktualizacji danych danych"),
  Close    FLOAT64  NOT NULL OPTIONS(description="Kurs zamknięcia instrumentu finansowego"),
  Volume   INT64    NOT NULL OPTIONS(description="Dzienny wolumen"),
  Turnover INT64    NOT NULL OPTIONS(description="Dzienny obrót")
);

-- Tabela przechowująca unikatowe instrumenty --
CREATE OR REPLACE TABLE `projekt-inwestycyjny.Dane_instrumentow.Instruments` (
  Instrument_id          INT64   OPTIONS(description="ID instrumentu finansowego - indeks"),
  Ticker                 STRING  OPTIONS(description="Ticker instrumentu finansowego"),
  Name                   STRING  OPTIONS(description="Nazwa instrumentu finansowego"),
  Unit                   INT64   OPTIONS(description="Jednoska rozliczeniowa instrumentu finansowego"),
  Country                STRING  OPTIONS(description="Kraj notowania instrumentu finansowego"),
  Market                 STRING  OPTIONS(description="Rynek instrumentu finansowego"),
  Currency               STRING  OPTIONS(description="Waluta instrumentu finansowego"),
  Distribution_policy    STRING  OPTIONS(description="Polityka dystrybucji instrumentu finansowego"),
  Instrument_type_id     INT64   OPTIONS(description="ID typu instrumentu finansowego"),
  Instrument_headquarter STRING  OPTIONS(description="Siedziba instrumentu finansowego"),
  Status                 INT64   OPTIONS(description="Status instrumentu finansowego")
);

-- Tabela przechowująca typy instrumentów -- 
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Dane_instrumentow.Instruments_types` (
  Instrument_type_id INT64 NOT NULL,
  Instrument_type STRING NOT NULL
);

-- Uzupełenie tabeli typami danych --
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (1, "Akcje polskie");
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (2, "Akcje zagraniczne");
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (3, "ETF polskie");
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (4, "ETF zagraniczne");
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (5, "Obligacje skarbowe");
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (6, "Obligacje zagraniczne");
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (7, "Obligacje zagraniczne");

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
  Dirty_bond_price FLOAT64,
  Tax_paid BOOL NOT NULL,
  Tax_value FLOAT64 NOT NULL
);

-- Tabela Transactions zawiera dane do rozliczeń podatkowy --
CREATE OR REPLACE TABLE `projekt-inwestycyjny.Transactions.Tax_calculations` (
  Date_sell DATE NOT NULL,
  Date_buy DATE,
  Investment_period INT64,
  Quantity INT64 NOT NULL,
  Buy_Price FLOAT64,
  Sell_Price FLOAT64 NOT NULL,
  Buy_currency FLOAT64,
  Sell_currency FLOAT64 NOT NULL,
  Currency STRING,
  Transaction_type STRING NOT NULL,
  Instrument_type STRING NOT NULL,
  Country STRING NOT NULL,
  Instrument_headquarter STRING NOT NULL,
  Ticker STRING NOT NULL,
  Ticker_id INT64 NOT NULL,
  Tax_deductible_expenses FLOAT64,
  Income FLOAT64,
  Profit FLOAT64,
  Tax_paid BOOL NOT NULL,
  Tax_value FLOAT64
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