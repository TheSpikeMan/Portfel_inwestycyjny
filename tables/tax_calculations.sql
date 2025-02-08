CREATE TABLE `projekt-inwestycyjny.Transactions.Tax_calculations`
(
  Date_sell               DATE NOT NULL,
  Date_buy                DATE,
  Investment_period       INT64,
  Quantity                INT64 NOT NULL,
  Buy_Price               FLOAT64,
  Sell_Price              FLOAT64 NOT NULL,
  Buy_currency            FLOAT64,
  Sell_currency           FLOAT64 NOT NULL,
  Currency                STRING,
  Transaction_type        STRING NOT NULL,
  Instrument_type         STRING NOT NULL,
  Country                 STRING NOT NULL,
  Instrument_headquarter  STRING NOT NULL,
  Ticker                  STRING NOT NULL,
  Ticker_id               INT64 NOT NULL,
  Tax_deductible_expenses FLOAT64,
  Income                  FLOAT64,
  Profit                  FLOAT64,
  Tax_paid                BOOL NOT NULL,
  Tax_value               FLOAT64
);
