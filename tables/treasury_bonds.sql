CREATE TABLE `projekt-inwestycyjny.Dane_instrumentow.Treasury_Bonds`
(
  Ticker              STRING  NOT NULL OPTIONS(description="Ticker instrumentu inwestycyjnego"),
  First_year_interest FLOAT64 NOT NULL OPTIONS(description="Marża w pierwszym roku"),
  Regular_interest    FLOAT64 NOT NULL OPTIONS(description="Marża w kolejnych latach")
);
