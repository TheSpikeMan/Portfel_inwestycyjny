/*
OPIS WIDOKU

W widoku tym zawarte są wszystkie aktualne instrumenty, tzn. takie, które znajdują się obecnie w portfelu.
*/

-- POBRANIE DANYCH --
/* 
W pierwszym kroku pobierane są dane transakcyjne, dane z giełdy oraz dane instrumentów, a także dane dywidendowe.
*/

WITH 
transaction_view_raw      AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),
daily                     AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),
instruments               AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instruments`),
instrument_types          AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instrument_types`),

-- Transaction view --
transaction_view AS (
  SELECT *
  FROM transaction_view_raw
),

-- Amount left per ticker --
--> Wyznaczenie obecnego wolumenu dla wszystkich posiadanych instrumentów
amount_left_per_ticker     AS (
  SELECT
    Project_id                                                                                          AS Project_id,
    Instrument_id                                                                                       AS instrument_id,
    --> Wyznaczamy pozostałą ilość danego instrumentu na podstawie okna analitycznego (maksymalna wartość różnicy między dwoma poniższymi wskaźnikami)
    MAX(transaction_date_buy_ticker_amount - cumulative_sell_amount_per_ticker) OVER last_transaction   AS amount_left_per_ticker
  FROM transaction_view
  WHERE TRUE
  AND Transaction_type_group                                                 = "Buy_amount" --> Analizujemy wyłącznie zakupy
  AND transaction_date_buy_ticker_amount - cumulative_sell_amount_per_ticker > 0            --> Interesują nas wyłącznie przypadki obecnie posiadanych instrumentów
  QUALIFY TRUE
    AND ROW_NUMBER() OVER last_transaction = 1
  WINDOW
    last_transaction AS (
      PARTITION BY
        Project_id,
        Instrument_id,
        Transaction_type_group
      ORDER BY
        Transaction_date DESC,
        Transaction_amount DESC
    )
),

amount_left_per_transaction AS (
  SELECT
    tvr.*,
    --> Pozostała aktualna ilość instrumentu
    alpt.amount_left_per_ticker                                                                        AS amount_left_per_ticker,
    --> Skumulowana ilość zakupów od najnowszej transacji
    SUM(transaction_amount) OVER sum_last_amount                                                       AS cumulative_buy_amount,
    CASE
      --> Jeżeli pozostała ilość jest większa lub równa niż skumulowana ilość zakupów zwróć całkowitą wartość wolumentu zakupowego
      WHEN alpt.amount_left_per_ticker >= SUM(transaction_amount) OVER sum_last_amount
      AND Transaction_type_group = "Buy_amount"
      THEN transaction_amount
      --> Jeżeli pozostała ilość jest mniejsza niż skumulowana ilość zakupów zwróć tymczasowo 0
      WHEN alpt.amount_left_per_ticker < SUM(transaction_amount) OVER sum_last_amount
      AND Transaction_type_group = "Buy_amount"
      THEN 0 
      END                           AS amount_left
  FROM transaction_view             AS tvr
  LEFT JOIN amount_left_per_ticker  AS alpt
  ON tvr.Project_id                 = alpt.Project_id
  AND tvr.instrument_id             = alpt.instrument_id
  WINDOW
    sum_last_amount AS (
      PARTITION BY
        tvr.Project_id,
        tvr.Instrument_id,
        Transaction_type_group
      ORDER BY
        Transaction_Date     DESC,
        tvr.Instrument_id    DESC
    )
),

amount_left_per_transaction_corrected AS (
  SELECT
    * EXCEPT(amount_left),
    CASE
    --> Rozważ jeszcze raz przypadki, w których pozostała ilość jest mniejsza niż skumulowana wartość zakupów
      WHEN amount_left_per_ticker < cumulative_buy_amount
      --> Przyjmuj, że pozostała ilość dla takich przypadków jest równa różnicy
      THEN amount_left_per_ticker - SUM(amount_left) OVER sum_last_amount
      ELSE amount_left
      END AS amount_left
  FROM amount_left_per_transaction
  WINDOW
    sum_last_amount AS (
      PARTITION BY
        Project_id,
        Instrument_id,
        Transaction_type_group
      ORDER BY
        Transaction_Date DESC,
        Instrument_id    DESC
    )
),

corrected_again AS (
  SELECT
    *,
    CASE 
      WHEN SUM(amount_left) OVER sum_last_amount <= amount_left_per_ticker
      THEN amount_left
      ELSE 0
      END AS transaction_amount_left
  FROM amount_left_per_transaction_corrected
  WINDOW
    sum_last_amount AS (
      PARTITION BY
        Project_id,
        Instrument_id,
        Transaction_type_group
      ORDER BY
        Transaction_Date DESC,
        Instrument_id    DESC
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
    Project_id                                                  AS Project_id,
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
  FROM corrected_again
  WHERE TRUE
    AND transaction_amount_left <> 0 
  GROUP BY ALL
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
    transaction_view.* EXCEPT (Ticker, Close, Project_id),
    transaction_view.Project_id                           AS Project_id,
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
    ON transaction_view.Project_id = present_instruments_view.Project_id
    AND transaction_view.Ticker = present_instruments_view.Ticker
  LEFT JOIN daily
    ON transaction_view.Ticker = daily.Ticker
    AND transaction_view.Transaction_date = daily.`Date`
  WHERE TRUE
    AND Transaction_type_group = 'Div_related_amount'
    AND present_instruments_view.minimum_buy_date < transaction_view.Transaction_date
  WINDOW
    ticker_window AS (
      PARTITION BY 
        transaction_view.Project_id,
        transaction_view.Ticker
    ),
    ticker_year_window AS (
      PARTITION BY 
        transaction_view.Project_id,
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
    dividend_selection.Project_id                   AS Project_id,
    dividend_selection.Ticker                       AS Ticker,
    ROUND(SUM(Transaction_value_pln), 2)            AS dividend_sum,
    ROUND(AVG(dividend_ratio_pct) * 
          MAX(dividend_frequency), 2)               AS avg_dividend_ratio_per_ticker_pct
  FROM dividend_selection
  GROUP BY ALL
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
    piv.Project_id                                        AS Project_id,
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
  ON piv.Project_id = inst.Project_id
  AND piv.Ticker = inst.Ticker
  LEFT JOIN dividend_sum AS div_sum
  ON piv.Project_id = div_sum.Project_id
  AND piv.Ticker = div_sum.Ticker
  LEFT JOIN instrument_types AS inst_typ
  ON inst.Instrument_type_id  = inst_typ.Instrument_type_id
)


SELECT * 
FROM present_instruments_plus_present_indicators
WHERE TRUE
ORDER BY Project_id, Ticker;
