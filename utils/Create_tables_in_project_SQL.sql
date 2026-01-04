-- Utworzenie datasetów

CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Calendar`;
CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Dane_instrumentow`;
CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Inflation`;
CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Transactions`;
CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Waluty`;
CREATE SCHEMA IF NOT EXISTS `projekt-inwestycyjny.Analysis`;

-- UTWORZENIE TABEL W ZBIORZE CALENDAR --
-- Tabela przechowuje zestaw dat od 2020-01-01 do 2030-12-31 wraz z innymi miarami takimi jak rok, miesiąć
-- czy kwartał lub dzień roboczy

CREATE OR REPLACE TABLE `projekt-inwestycyjny.Calendar.Dates`
  (
    date           DATE   OPTIONS(description="Pełna data w formacie YYYY-MM-DD"),
    year           INT64  OPTIONS(description="Rok danej daty (np. 2025)"),
    month          INT64  OPTIONS(description="Numer miesiąca (1 = styczeń, 12 = grudzień)"),
    day            INT64  OPTIONS(description="Dzień miesiąca (1–31)"),
    quarter        INT64  OPTIONS(description="Numer kwartału (1–4)"),
    quarter_text   STRING OPTIONS(description="Nazwa kwartału po polsku, np. 'III kwartał'"),
    year_quarter   STRING OPTIONS(description="Połączenie roku i kwartału, np. '2025 Q3'"),
    week           INT64  OPTIONS(description="Numer tygodnia wg ISO 8601 (poniedziałek = 1)"),
    weekday        INT64  OPTIONS(description="Dzień tygodnia jako liczba od 0 (niedziela) do 6 (sobota)"),
    is_working_day INT64  OPTIONS(description="1 = dzień roboczy (poniedziałek–piątek), 0 = weekend")
  ) 
  AS
  SELECT
    dates                              AS date,
    EXTRACT(YEAR FROM dates)           AS year,
    EXTRACT(MONTH FROM dates)          AS month,
    EXTRACT(DAY FROM dates)            AS day,
    EXTRACT(QUARTER FROM dates)        AS quarter,
    CASE
      WHEN EXTRACT(QUARTER FROM dates) = 1 THEN "I kwartał"
      WHEN EXTRACT(QUARTER FROM dates) = 2 THEN "II kwartał"
      WHEN EXTRACT(QUARTER FROM dates) = 3 THEN "III kwartał"
      WHEN EXTRACT(QUARTER FROM dates) = 4 THEN "IV kwartał"
    END                                AS quarter_text,
    CONCAT(
      SAFE_CAST(EXTRACT(YEAR FROM dates) AS STRING),
      " Q",
      SAFE_CAST(EXTRACT(QUARTER FROM dates) AS STRING)
    )                                  AS year_quarter,
    EXTRACT(ISOWEEK FROM dates)        AS week,
    EXTRACT(DAYOFWEEK FROM dates) - 1  AS weekday,
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM dates) - 1
      BETWEEN 1 AND 5
      THEN 1
      ELSE 0
      END                              AS is_working_day
  FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2030-12-31', INTERVAL 1 DAY) ) AS dates
  ORDER BY
    dates DESC


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
  project_id             INT64   OPTIONS(description="ID portfela inwestycyjnego"),
  instrument_id          INT64   OPTIONS(description="ID instrumentu finansowego - indeks"),
  ISIN                   STRING  OPTIONS(description="Międzynarodowy numer identyfikacyjny papierów wartościowych"),
  ticker                 STRING  OPTIONS(description="Ticker instrumentu finansowego"),
  name                   STRING  OPTIONS(description="Nazwa instrumentu finansowego"),
  unit                   INT64   OPTIONS(description="Jednoska rozliczeniowa instrumentu finansowego"),
  country                STRING  OPTIONS(description="Kraj notowania instrumentu finansowego"),
  market                 STRING  OPTIONS(description="Rynek instrumentu finansowego"),
  market_currency        STRING  OPTIONS(description="Waluta instrumentu finansowego na danym rynku"),
  ticker_currency        STRING  OPTIONS(description="Waluta bazowa, w jakiej instrument jest wewnętrznie rozliczany"),
  distribution_policy    STRING  OPTIONS(description="Polityka dystrybucji instrumentu finansowego"),
  instrument_type_id     INT64   OPTIONS(description="ID typu instrumentu finansowego"),
  instrument_headquarter STRING  OPTIONS(description="Siedziba instrumentu finansowego"),
  status                 INT64   OPTIONS(description="Status instrumentu finansowego")
);

-- Tabela przechowująca typy instrumentów -- 
CREATE TABLE IF NOT EXISTS `projekt-inwestycyjny.Dane_instrumentow.Instrument_types` 
(
  Instrument_type_id   INT64 NOT NULL  OPTIONS(description="ID typu instrumentu finansowego - indeks"),
  Instrument_type      STRING NOT NULL OPTIONS(description="Nazwa typu instrumentu finansowego"),
  Instrument_type_main STRING NOT NULL OPTIONS(description="Nazwa typu głównego instrumentu finansowego")
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