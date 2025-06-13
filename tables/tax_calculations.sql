CREATE OR REPLACE TABLE `projekt-inwestycyjny.Transactions.Tax_calculations`
(
  Project_id              INT64 NOT NULL   OPTIONS(description="ID portfela inwestycyjnego"),
  Date_sell               DATE NOT NULL    OPTIONS(description="Data sprzedaży instrumentu"),
  Date_buy                DATE             OPTIONS(description="Data zakupu instrumentu"),
  Investment_period       INT64            OPTIONS(description="Okres inwestowania"),
  Quantity                INT64 NOT NULL   OPTIONS(description="Ilość"),
  Buy_Price               FLOAT64          OPTIONS(description="Cena zakupu"),
  Sell_Price              FLOAT64 NOT NULL OPTIONS(description="Cena sprzedaży"),
  Buy_currency            FLOAT64          OPTIONS(description="Kurs zakupu waluty wobec PLN"),
  Sell_currency           FLOAT64 NOT NULL OPTIONS(description="Kurs sprzedaży waluty wobec PLN"),
  Currency                STRING           OPTIONS(description="Waluta transakcji"),
  Transaction_type        STRING NOT NULL  OPTIONS(description="Rodzaj transakcji"),
  Instrument_type         STRING NOT NULL  OPTIONS(description="Typ instrumentu"),
  Country                 STRING NOT NULL  OPTIONS(description="Kurs zakupu waluty wobec PLN"),
  Instrument_headquarter  STRING NOT NULL  OPTIONS(description="Waluta instrumentu finansowego na danym rynku"),
  Ticker                  STRING NOT NULL  OPTIONS(description="Symbol instrumentu finansowego"),
  Ticker_id               INT64 NOT NULL   OPTIONS(description="ID instrumentu finansowego - indeks"),
  Tax_deductible_expenses FLOAT64          OPTIONS(description="Koszt osiągnięcia przychodu"),
  Income                  FLOAT64          OPTIONS(description="Przychód"),
  Profit                  FLOAT64          OPTIONS(description="Zysk"),
  Tax_paid                BOOL NOT NULL    OPTIONS(description="Flaga zapłacenia podatku"),
  Tax_value               FLOAT64          OPTIONS(description="Wartość zapłaconego podatku")
);
