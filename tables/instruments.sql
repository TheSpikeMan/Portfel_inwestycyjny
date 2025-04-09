CREATE OR REPLACE TABLE `projekt-inwestycyjny.Dane_instrumentow.Instruments`
(
  project_id             INT64  OPTIONS(description="ID portfela inwestycyjnego"),
  instrument_id          INT64  OPTIONS(description="ID instrumentu finansowego - indeks"),
  ISIN                   STRING OPTIONS(description="Międzynarodowy numer identyfikacyjny papierów wartościowych"),
  ticker                 STRING OPTIONS(description="Symbol instrumentu finansowego"),
  name                   STRING OPTIONS(description="Nazwa instrumentu finansowego"),
  unit                   INT64  OPTIONS(description="Jednoska rozliczeniowa instrumentu finansowego"),
  country                STRING OPTIONS(description="Kraj notowania instrumentu finansowego"),
  market                 STRING OPTIONS(description="Rynek instrumentu finansowego"),
  market_currency        STRING OPTIONS(description="Waluta instrumentu finansowego na danym rynku"),
  ticker_currency        STRING OPTIONS(description="Waluta bazowa, w jakiej instrument jest wewnętrznie rozliczany"),
  distribution_policy    STRING OPTIONS(description="Polityka dystrybucji instrumentu finansowego"),
  instrument_type_id     INT64  OPTIONS(description="ID typu instrumentu finansowego"),
  instrument_headquarter STRING OPTIONS(description="Siedziba instrumentu finansowego"),
  status                 INT64  OPTIONS(description="Status instrumentu finansowego")
);
