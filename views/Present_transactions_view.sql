/*
OPIS WIDOKU

W widoku tym zawarte są wszystkie aktualne instrumenty, tzn. takie, które znajdują się obecnie w portfelu.
*/

-- POBRANIE DANYCH --
/* 
W pierwszym kroku pobierane są dane transakcyjne, dane z giełdy oraz dane instrumentów, a także dane dywidendowe.
*/

WITH 
transaction_view          AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),
daily                     AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),
instruments               AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instruments`),
instrument_types          AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instrument_types`),

-- INITIAL AGGREGATION --
/*
W tym kroku wyciągane są wszystkie transakcje i wykonywane jest działanie mające na celu określenie pozostałej ilości
w posiadaniu, dla danej transakcji zakupu.
Sprawdzanie są po kolei 4 warunki:
- Jeżeli analizowaną transakcją jest transakcja zakupowa i skumulowana ilość zakupiona jest mniejsza niż ilość
sprzedana, oznacza to, że dana transakcja została sprzedana całkowicie.
- Dla wszystkich transakcji związanych ze sprzedażami i wypłatą dywidendy/odsetek, wpisujemy od razu 0.
- Jeżeli dana ilość zakupiona w danym momencie jest większa niż całkowita ilość sprzedana i jest to pierwsza tego
transakcja oraz nastąpiła jakakolwiek sprzedaż oblicza pozostałą ilość jako różnicę w całkowitej ilości zakupionej
do danego momentu i sprzedanej in total.
- W pozostałych przypadkach podaje wartość zakupioną w danej transakcji.
*/

initial_aggregation AS (
SELECT
  Transaction_id                      AS Transaction_id,
  Transaction_date                    AS Transaction_date,
  Ticker                              AS Ticker,
  Name                                AS Name,
  Currency                            AS Currency,
  age_of_instrument                   AS age_of_instrument,
  Transaction_price                   AS Transaction_price,
  Transaction_amount                  AS Transaction_amount,
  Transaction_value_pln               AS Transaction_value_pln,
  Transaction_type_group              AS Transaction_type_group,
  transaction_date_ticker_amount      AS transaction_date_ticket_amount,
  transaction_date_ticker_value       AS transaction_date_ticker_value,
  transaction_date_buy_ticker_amount  AS transaction_date_ticket_amount,
  cumulative_sell_amount_per_ticker   AS cumulative_sell_amount_per_ticker,
  last_currency_close                 AS last_currency_close,
  CASE
    WHEN Transaction_type_group                                                = "Buy_amount"
    AND transaction_date_buy_ticker_amount - cumulative_sell_amount_per_ticker <= 0
    THEN 0
    WHEN Transaction_type_group IN ("Sell_amount", "Div_related_amount")
    THEN 0
    WHEN ROW_NUMBER() OVER last_ticker_transaction_window                      = 1
    AND transaction_date_buy_ticker_amount - cumulative_sell_amount_per_ticker > 0
    AND cumulative_sell_amount_per_ticker                                      <> 0
    THEN transaction_date_buy_ticker_amount - cumulative_sell_amount_per_ticker
    ELSE Transaction_amount
    END                               AS transaction_amount_left
FROM transaction_view
WHERE TRUE
WINDOW
  last_ticker_transaction_window AS (
    PARTITION BY Ticker
    ORDER BY Transaction_date ASC, Transaction_id ASC
  )
),

-- PRESENT INSTRUMENTS VIEW --
/*
W tym kroku odfiltrowane są wszystkie sprzedane instrumenty (transaction_amount_left <> 0 )
Dodatkowo wyznaczane są następujące parametry:
- Aktualny wolumen - ticker_present_amount
- Wartość zakupu - ticker_buy_value
- Średnia cena zakupu - ticker_average_close
- Maksymalną liczbę dni od zakupu danego instrumentu - max_age_of_instrument
- Minimalną datę zakupu w obrębie aktualnych instrumentów - minimum_buy_date
*/

present_instruments_view AS (
  SELECT
    Ticker                                                      AS Ticker,
    Name                                                        AS Name,
    Currency                                                    AS currency_exposure,
    SUM(transaction_amount_left)                                AS ticker_present_amount, 
    MAX(age_of_instrument)                                      AS max_age_of_instrument,
    ROUND(
      SUM(transaction_amount_left * transaction_price * last_currency_close), 
      2)                                                        AS ticker_buy_value,
    ROUND(
      SUM(transaction_amount_left * transaction_price * last_currency_close)/
      SUM(transaction_amount_left), 
      2)                                                        AS ticker_average_close,
    MIN(Transaction_date)                                       AS minimum_buy_date
  FROM initial_aggregation
  WHERE
    transaction_amount_left <> 0 
  GROUP BY
    Ticker,
    Name,
    Currency
),

-- DAILY DATA --
/*
W tym kroku wyciągane są ostatnie dane z giełdy, dzięki którym możliwe jest wyznaczenie aktualnego kursu 
dla danego instrumentu
*/

daily_data AS (
  SELECT
    Ticker  AS Ticker,
    `Date`  AS `Date`,
    Close   AS Close
  FROM daily
  QUALIFY TRUE
    AND ROW_NUMBER() OVER last_ticker_transaction = 1
  WINDOW
    last_ticker_transaction AS (
      PARTITION BY Ticker
      ORDER BY `Date` DESC
    )
),

--- DYWIDENDY ---

-- DIVIDEND SELECTION -- 
/*
W bieżącym kroku dokonywana jest analiza wszystkich transakcji dywidendowych, dla których data 
wypłaty dywidendy jest większa od daty zakupu danego tickera, do którego przynależy dywidenda. 
Dodatkowo liczony jest wskaźnik stopy dywidendy (na podstawie zestawienia ceny zamknięcia 
instrumentu, na moment wypłaty dywidendy).
*/

dividend_selection AS (
  SELECT
    transaction_view.* EXCEPT (Ticker, Close),
    transaction_view.Ticker                               AS Ticker,
    COALESCE(transaction_view.Close, 0)                   AS Close,
    COALESCE(
        ROUND(100 * 
          SAFE_DIVIDE(Transaction_price * Currency_close , 
                      transaction_view.Close * Unit), 
            2),
            0)                                            AS dividend_ratio_pct,
    present_instruments_view.minimum_buy_date             AS minimum_buy_date,
    SUM(Transaction_value_pln) OVER ticker_window         AS dividend_sum, -- do sprawdzenia
    COUNT(Transaction_id)      OVER ticker_year_window    AS dividend_frequency
  FROM transaction_view
  LEFT JOIN present_instruments_view
  ON transaction_view.Ticker = present_instruments_view.Ticker
  LEFT JOIN daily
  ON transaction_view.Ticker = daily.Ticker
  AND transaction_view.Transaction_date = daily.`Date`
  WHERE TRUE
    AND Transaction_type_group = 'Div_related_amount'
    AND present_instruments_view.minimum_buy_date < transaction_view.Transaction_date
  WINDOW
    ticker_window AS (
      PARTITION BY transaction_view.Ticker
    ),
    ticker_year_window AS (
      PARTITION BY 
        transaction_view.Ticker,
        EXTRACT(YEAR FROM Transaction_date)
    )
  ),

-- DIVIDEND SUM --
/*
Wyciągnięcie sumy wartości dywidend dla danego tickera oraz średniego dividend ratio.
W widoku wzięta jest pod uwagę częstotliwość wypłaty dywidendy - wskaźnik średniej dywidendy mnożony jest przez częstotliwość wypłaty.
*/

dividend_sum AS (
  SELECT
    dividend_selection.Ticker                       AS Ticker,
    ROUND(SUM(Transaction_value_pln), 2)            AS dividend_sum,
    ROUND(AVG(dividend_ratio_pct) * 
          MAX(dividend_frequency), 2)               AS avg_dividend_ratio_per_ticker_pct
  FROM dividend_selection
  GROUP BY
    Ticker
  ),

--- FINALNA AGREGACJA I PREZENTACJA DANYCH ---


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
- avg_dividend_ratio_per_ticker_pct - średnia stopa dywidendy instrumentu (roczna)
*/

present_instruments_plus_present_indicators AS (
  SELECT
    inst.Ticker                                           AS Ticker,
    inst_typ.Instrument_type                              AS instrument_class,
    currency_exposure                                     AS currency_exposure,
    inst.Name                                             AS Name,
    piv.ticker_present_amount                             AS ticker_present_amount,
    piv.ticker_average_close                              AS ticker_average_close,
    piv.ticker_buy_value                                  AS ticker_buy_value,
    ROUND(
        (piv.ticker_present_amount 
        * daily.Close 
        * inst.unit), 
      2)                                                  AS ticker_present_value,
    daily.Close * inst.unit                               AS current_price,
    daily.Date                                            AS current_price_date,
    piv.max_age_of_instrument                             AS max_age_of_instrument,
    ROUND(100 * 
        (piv.ticker_present_amount 
        * daily.Close 
        * inst.unit)/
          SUM(piv.ticker_present_amount * daily.Close * inst.unit) OVER(), 
      2) 
                                                          AS share_of_portfolio,
    ROUND(100 * 
      ((piv.ticker_present_amount * daily.Close * inst.unit) /
        piv.ticker_buy_value) - 100, 
      2) 
                                                          AS rate_of_return,
    CASE
      WHEN piv.max_age_of_instrument > 120 
        THEN ROUND(
              (365 * (100 * ((piv.ticker_present_amount * daily.Close * inst.unit)/piv.ticker_buy_value) - 100))
              /piv.max_age_of_instrument, 2)
    ELSE 0
    END                                                   AS  yearly_rate_of_return,
    CASE
      WHEN piv.max_age_of_instrument > 120 
      THEN IFNULL(ROUND((365 * (100 * ((piv.ticker_present_amount * daily.Close * inst.unit + div_sum.dividend_sum)/piv.ticker_buy_value) - 100))
      /piv.max_age_of_instrument, 2), ROUND((365 * (100 * ((piv.ticker_present_amount * daily.Close * inst.unit)/piv.ticker_buy_value) - 100))
      /piv.max_age_of_instrument, 2))  
    ELSE 0
    END                                                   AS yearly_rate_of_return_incl_div,
    ROUND((daily.Close * inst.unit - piv.ticker_average_close) * ticker_present_amount, 2) 
                                                          AS profit,
    IFNULL(ROUND(div_sum.dividend_sum + (daily.Close * inst.unit - piv.ticker_average_close) * piv.ticker_present_amount, 2),
      ROUND((daily.Close * inst.unit - piv.ticker_average_close) * piv.ticker_present_amount, 2))  
                                                          AS profit_incl_div,
    IFNULL(div_sum.avg_dividend_ratio_per_ticker_pct, 0) 
                                                          AS avg_dividend_ratio_per_ticker_pct
  FROM present_instruments_view AS piv
  LEFT JOIN Daily_data AS daily
  ON piv.Ticker = daily.Ticker
  INNER JOIN instruments AS inst
  ON piv.Ticker = inst.Ticker
  LEFT JOIN dividend_sum AS div_sum
  ON piv.Ticker = div_sum.Ticker
  LEFT JOIN instrument_types AS inst_typ
  ON inst.Instrument_type_id  = inst_typ.Instrument_type_id
)

SELECT * 
FROM present_instruments_plus_present_indicators
WHERE TRUE
ORDER BY Ticker;
