/*
OPIS WIDOKU

W widoku tym zawarte są wszystkie aktualne instrumenty, tzn. takie, które znajdują się obecnie w portfelu.
*/

-- POBRANIE DANYCH --
/* 
W pierwszym kroku pobierane są dane transakcyjne, dane z giełdy oraz dane instrumentów, a także dane dywidendowe.
*/

WITH 
transaction_view AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),
daily AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),
instruments AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instruments`),
instrument_types AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instrument_types`),
dividend_view AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Dividend_view`),

-- INITIAL AGGREGATION --
/*
W kroku tym każdej transakcji przyporządkowany jest numer, którego zasada przydzielania jest następująca:
- Stwórz okno dla każdego Tickera,
- Wszystkie wiersze ułóż malejąco wg daty transakcji,
- Wszystkim transakcjom przypisz numerację, od najnowszej transakcji do najstarszej

W kroku tym wyciągana jest ostatnia operacja (zakup, sprzedaż instrumentu) dla danego Tickera.
*/

initial_aggregation AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY Ticker ORDER BY Transaction_date DESC) AS last_transaction_id
  FROM
    transaction_view
  WHERE
    Transaction_type IN ('Buy', 'Sell')
),


-- MED AGGREGATION --
/*
Wyciągnięcie tickerów instrumentów, które znajdują się w aktualnym porfelu.
Uwzględniamy tylko takie, które posiadają niezerowe wolumeny.
*/

med_aggregation AS (
  SELECT
    Ticker AS Ticker,
  FROM
    initial_aggregation
  WHERE
    last_transaction_id = 1 AND
    transaction_date_ticker_amount <> 0
),

-- INTERMEDIATE AGGREGATION --
/*
W kroku tym zestawiana jest kumulowana ilość zakupów dane instrumentów, z łączną ilością sprzedaży.
Na tej podstawie wyznaczany jest wskaźnik, który przyjmuje dwie wartości:
- "Sprzedany", jeśli instrument z danej tranakcji został sprzedany,
- "Aktualny", jeśli instrument z danej transakcji wciąż znajduje się w portfelu.
*/

intermediate_aggregation AS (
  SELECT
    * EXCEPT(Ticker),
    med_aggregation.Ticker,
    CASE
      WHEN (transaction_date_buy_ticker_amount - cumulative_sell_amount_per_ticker <= 0) THEN "Sprzedany"
    ELSE "Aktualny"
    END AS transaction_status
  FROM
    transaction_view
  INNER JOIN med_aggregation
  ON transaction_view.Ticker = med_aggregation.Ticker
  WHERE
    transaction_view.Transaction_Type IN ("Buy", "Sell")
),


-- PRESENT INSTRUMENTS VIEW --
/*
W tym kroku odfiltrowane są wszystkie sprzedane instrumenty.
Dodatkowo wyznaczane są następujące parametry:
- Aktualny wolumen,
- Wartość zakupu,
- Średnia cena zakupu,
- Maksymalną liczbę dni od zakupu danego instrumentu.
*/

present_instruments_view AS (
  SELECT
    Ticker,
    Name,
    Currency AS currency_exposure,
    MAX(transaction_date_ticker_amount) AS ticker_present_amount,
    MAX(age_of_instrument) AS max_age_of_instrument,
    ROUND(SUM(transaction_value_pln), 2) AS ticker_buy_value,
    ROUND(SUM(transaction_value_pln)/MAX(transaction_date_ticker_amount), 2) AS ticker_average_close
  FROM intermediate_aggregation
  WHERE
    transaction_status = "Aktualny"
  GROUP BY
    1,2,3
  ORDER BY
    1,2,3
),

-- DAILY DATA --
/*
W tym kroku wyciągane są ostatnie dane z giełdy, dzięki którym możliwe jest wyznaczenie aktualnego kursu, wolumenu i obrotu dla danego instrumentu
*/

daily_data AS (
  SELECT
  * EXCEPT(row_num)
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY Ticker ORDER BY `Date` DESC) AS row_num
    FROM
      daily
  )
  WHERE
    row_num = 1
),

-- DIVIDEND SUM --
/*
Wyciągnięcie sumy wartości dywidend dla danego tickera oraz średniego dividend ratio.
*/
dividend_sum AS (
  SELECT
    Ticker AS Ticker,
    --ROUND(SUM(Transaction_value_pln), 2) AS dividend_sum
    MAX(divident_sum_total_per_ticker) AS dividend_sum,
    MAX(avg_dividend_ratio_per_ticker_pct) AS avg_dividend_ratio_per_ticker_pct
  FROM
    dividend_view
  GROUP BY
    Ticker
),

-- PRESENT INSTRUMENTS PLUS PRESENT INDICATORS --
/*
W tym kroku połączone są dane portfelowe z danymi giełdowymi oraz danymi instrumentów i danymi dywidentowymi i liczone są wskaźniki:
- ticker_average_close - średnia cena zakupu,
- ticker_buy_values - sumaryczna cena zakupu,
- ticker_present_value - obecna wartość danego instrumentu
- current_price - najnowsza cena danego instrumentu  z uwzględnieniem jednostki (unit)
- current_price_date - data aktualizacji aktualnych danych giełdowych,
- max_age_of_instrument - okres inwestowania
- share_of_portfolio - udział wartości instrumentu w ogólnej wartości portfela,
- yearly_rate_of_return - roczna stopa zwrotu instruemntu, bez uwzglęnienia dywidend, odsetek i podatku
- yearly_rate_of_return_incl_div - roczna stopa zwrotu instrument, z uwzględnieniem dywident, bez uwzględnienia podatku
- rate_of_return - stopa zwrotu instrumentu, bez uwzględnienia dywidend, odsetek i podatku,
- profit - niezrealizowany zysk transakcyjny - zysk wynikający z różnicy kursowej, bez uwzględnienia dywidend i odsetek
- profit_incl_div - niezrealizowany zysk transakcyjny - zysk wynikający z różnicy kursowej, z uwzglęnieniem dywident, bez uwzględnienia odsetek
- avg_dividend_ratio_per_ticker_pct - średnia stopa dywidendy instrumentu
*/

present_instruments_plus_present_indicators AS (
  SELECT
    instruments.Ticker,
    Instrument_types.Instrument_type AS instrument_class,
    currency_exposure AS currency_exposure,
    instruments.Name AS Name,
    present_instruments_view.ticker_present_amount AS ticker_present_amount,
    present_instruments_view.ticker_average_close AS ticker_average_close,
    present_instruments_view.ticker_buy_value AS ticker_buy_value,
    ROUND((ticker_present_amount * Close * instruments.unit), 2) AS ticker_present_value,
    daily_data.Close * instruments.unit AS current_price,
    daily_data.`Date` AS current_price_date,
    present_instruments_view.max_age_of_instrument AS max_age_of_instrument,
    ROUND(100 * (ticker_present_amount * Close * instruments.unit)/SUM(ticker_present_amount * Close * instruments.unit) OVER(), 2) 
      AS share_of_portfolio,
    ROUND(100 * ((ticker_present_amount * Close * instruments.unit)/ticker_buy_value) - 100, 2) AS rate_of_return,
    CASE
    WHEN present_instruments_view.max_age_of_instrument > 120 THEN
    ROUND((365 * (100 * ((ticker_present_amount * Close * instruments.unit)/ticker_buy_value) - 100))
      /max_age_of_instrument, 2)
    ELSE 0
    END AS yearly_rate_of_return,
    CASE
    WHEN present_instruments_view.max_age_of_instrument > 120 THEN
    IFNULL(ROUND((365 * (100 * ((ticker_present_amount * Close * instruments.unit + dividend_sum.dividend_sum)/ticker_buy_value) - 100))
      /max_age_of_instrument, 2), ROUND((365 * (100 * ((ticker_present_amount * Close * instruments.unit)/ticker_buy_value) - 100))
      /max_age_of_instrument, 2))
    ELSE 0
    END AS yearly_rate_of_return_incl_div,
    ROUND((Close * instruments.unit - ticker_average_close) * ticker_present_amount, 2) AS profit,
    IFNULL(ROUND(dividend_sum.dividend_sum + (Close * instruments.unit - ticker_average_close) * ticker_present_amount, 2),
      ROUND((Close * instruments.unit - ticker_average_close) * ticker_present_amount, 2))  AS profit_incl_div,
    IFNULL(dividend_sum.avg_dividend_ratio_per_ticker_pct, 0) AS avg_dividend_ratio_per_ticker_pct
  
  FROM
    present_instruments_view
  LEFT JOIN Daily_data
  ON present_instruments_view.Ticker = Daily_data.Ticker
  INNER JOIN instruments
  ON present_instruments_view.Ticker = instruments.Ticker
  LEFT JOIN dividend_sum
  ON present_instruments_view.Ticker = dividend_sum.Ticker
  LEFT JOIN instrument_types
  ON instruments.Instrument_type_id = instrument_types.Instrument_type_id

)

SELECT * FROM present_instruments_plus_present_indicators ORDER BY Ticker;
