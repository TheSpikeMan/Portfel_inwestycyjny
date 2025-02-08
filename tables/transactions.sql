CREATE TABLE `projekt-inwestycyjny.Transactions.Transactions`
(
  Project_id         INT64   OPTIONS(description="ID portfela inwestycynego"),
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
