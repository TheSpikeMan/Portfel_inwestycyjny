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
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Dane_instrumentow.Instrument_types` 
(
  Instrument_type_id INT64 NOT NULL  OPTIONS(description="ID typu instrumentu finansowego - indeks"),
  Instrument_type    STRING NOT NULL OPTIONS(description="Nazwa typu instrumentu finansowego")
);

-- Uzupełenie tabeli typami danych --
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (1, "Akcje polskie");
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (2, "Akcje zagraniczne");
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (3, "ETF polskie");
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (4, "ETF zagraniczne");
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (5, "Obligacje skarbowe");
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (6, "Obligacje zagraniczne");
INSERT INTO `Dane_instrumentow.Instrument_types` VALUES (7, "Obligacje zagraniczne");

-- Tabela przechowująca informację o marży obligacji skarbowych --
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Dane_instrumentow.Treasury_Bonds`
(
  Ticker              STRING NOT NULL  OPTIONS(description="Ticker instrumentu finansowego"),
  First_year_interest FLOAT64 NOT NULL OPTIONS(description="Marża w pierwszym roku"),
  Regular_interest    FLOAT64 NOT NULL OPTIONS(description="Marża w kolejnych latach")
);

-- W zbiorze znajduje się również widok 'Trends_analysis'

-- UTWORZENIE TABEL W ZBIORZE INFLATION -- 
-- Tabela Inflation zawiera dane inflacyjne scrapowane co miesiąc --
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Inflation.Inflation`(
  `date`    STRING  NOT NULL OPTIONS(description="Data"),
  inflation FLOAT64 NOT NULL OPTIONS(description="Inflacja")
);

-- UTWORZENIE TABEL W ZBIORZE TRANSACTIONS --
-- Tabela Transactions zawiera dane transakcyjne --

CREATE OR REPLACE TABLE `projekt-inwestycyjny.Transactions.Transactions`
(
  Transaction_id     INT64   OPTIONS(description="ID transakcji - indeks"),
  Transaction_date   DATE    OPTIONS(description="Data transakcji"),
  Transaction_type   STRING  OPTIONS(description="Typ transakcji"),
  Currency           STRING  OPTIONS(description="Waluta"),
  Transaction_price  FLOAT64 OPTIONS(description="Cena transakcyjna"),
  Transaction_amount FLOAT64 OPTIONS(description="Liczba"),
  Instrument_id      INT64   OPTIONS(description="ID instrumentu finansowego"),
  Commision_id       FLOAT64 OPTIONS(description="Prowizja"),
  Dirty_bond_price   FLOAT64 OPTIONS(description="Cena brudna"),
  Tax_paid           BOOL    OPTIONS(description="Podatek zapłacony"),
  Tax_value          FLOAT64 OPTIONS(description="Wartość podatku")
);

-- Tabela Transactions zawiera dane do rozliczeń podatkowy --
CREATE OR REPLACE TABLE `projekt-inwestycyjny.Transactions.Tax_calculations` (
  Date_sell               DATE    NOT NULL OPTIONS(description="Data sprzedaży instrumentu finansowego"),
  Date_buy                DATE             OPTIONS(description="Data zakupu instrumentu finansowego"),
  Investment_period       INT64            OPTIONS(description="Okres inwestowania - liczba dni"),
  Quantity                INT64   NOT NULL OPTIONS(description="Ilość"),
  Buy_Price               FLOAT64          OPTIONS(description="Cena zakupu"),
  Sell_Price              FLOAT64 NOT NULL OPTIONS(description="Cena sprzedaży"),
  Buy_currency            FLOAT64          OPTIONS(description="Wartość waluty podczas zakupu"),
  Sell_currency           FLOAT64 NOT NULL OPTIONS(description="Wartość waluty podczas sprzedaży"),
  Currency                STRING           OPTIONS(description="Waluta"),
  Transaction_type        STRING  NOT NULL OPTIONS(description="Typ transakcji"),
  Instrument_type         STRING  NOT NULL OPTIONS(description="Nazwa typu instrumentu finansowego"),
  Country                 STRING  NOT NULL OPTIONS(description="Kraj notowania instrumentu finansowego"),
  Instrument_headquarter  STRING  NOT NULL OPTIONS(description="Siedziba instrumentu finansowego"),
  Ticker                  STRING  NOT NULL OPTIONS(description="Ticker instrumentu finansowego"),
  Ticker_id               INT64   NOT NULL OPTIONS(description="ID instrumentu finansowego"),
  Tax_deductible_expenses FLOAT64          OPTIONS(description="Koszty uzyskania przychodu"),
  Income                  FLOAT64          OPTIONS(description="Przychód"),
  Profit                  FLOAT64          OPTIONS(description="Zysk"),
  Tax_paid                BOOL NOT NULL    OPTIONS(description="Podatek zapłacony"),
  Tax_value               FLOAT64          OPTIONS(description="Wartość podatku")
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
  Currency_date  DATE   NOT NULL OPTIONS(description="Data"),
  Currency       STRING NOT NULL OPTIONS(description="Waluta"),
  Currency_close FLOAT  NOT NULL OPTIONS(description="Wartość kursu walutowego wzg. PLN"),
);