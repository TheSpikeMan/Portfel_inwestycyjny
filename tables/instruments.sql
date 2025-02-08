CREATE TABLE `projekt-inwestycyjny.Dane_instrumentow.Instruments`
(
  Instrument_id          INT64  OPTIONS(description="ID instrumentu finansowego - indeks"),
  Ticker                 STRING OPTIONS(description="Ticker instrumentu finansowego"),
  Name                   STRING OPTIONS(description="Nazwa instrumentu finansowego"),
  Unit                   INT64  OPTIONS(description="Jednoska rozliczeniowa instrumentu finansowego"),
  Country                STRING OPTIONS(description="Kraj notowania instrumentu finansowego"),
  Market                 STRING OPTIONS(description="Rynek instrumentu finansowego"),
  Currency               STRING OPTIONS(description="Waluta instrumentu finansowego"),
  Distribution_policy    STRING OPTIONS(description="Polityka dystrybucji instrumentu finansowego"),
  Instrument_type_id     INT64  OPTIONS(description="ID typu instrumentu finansowego"),
  Instrument_headquarter STRING OPTIONS(description="Siedziba instrumentu finansowego"),
  Status                 INT64  OPTIONS(description="Status instrumentu finansowego")
);
