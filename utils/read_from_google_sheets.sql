CREATE OR REPLACE EXTERNAL TABLE `projekt-inwestycyjny.Extra.read_from_google_sheets` (
    flag_id         INT64   OPTIONS(description="Id flagi"),
    flag_name       STRING  OPTIONS(description="Nazwa flagi"),
    flag_Value      STRING  OPTIONS(description="Wartość flagi")
)

OPTIONS(
    sheet_range="Tab!A1:C",
    skip_leading_rows=1,
    format="GOOGLE_SHEETS",
    uris=["https://docs.google.com/spreadsheets/d/1yFGWzOohO2BlTAy4dn2GHBCkDc_sQBrj5YBLPhI9CiY/edit?gid=0#gid=0"]
)