"""
W kodzie zawarte są instrukcje do wykonywania operacji na zbiorach
danych w BigQuery, min. tworzenie tabel i widoków, odczyt danych z BQ.

"""

# ----------------------------------------------------------
# 1. Utworzenie datasetu w danym projekcie.
# ----------------------------------------------------------

from google.cloud import bigquery

# Definicja parametrów

project = 'projekt-inwestycyjny'
location = 'europe-central2'
nazwa_dataSetu = 'Test2'


# Utworzenie klienta
client = bigquery.Client(
    project = project,
    location = location
    )

# Utworzenie Datasetu
dataset = client.create_dataset(
    bigquery.Dataset(project + "." + nazwa_dataSetu)
    )

# ----------------------------------------------------------
# 2. Utworzenie tabeli w danym projekcie i zbiorze.
# ----------------------------------------------------------

from google.cloud import bigquery

# Definicja parametrów

project = 'projekt-inwestycyjny'
location = 'europe-central2'
nazwa_datasetu = 'Dane_instrumentow'
nazwa_tabeli = "Treasury_Bonds"

# Utworzenie klienta
client = bigquery.Client(
    project = project,
    location = location
    )

# Utworzenie Tabeli w danym Datasecie
table = client.create_table(
    bigquery.Table(table_ref = (project + "." + nazwa_datasetu + "." + nazwa_tabeli),
                   schema = (bigquery.SchemaField(name="Ticker", field_type="STRING", mode = "REQUIRED"),
                       bigquery.SchemaField(name="First_year_interest", field_type="FLOAT", mode = "REQUIRED"),
                       bigquery.SchemaField(name="Regular_interest", field_type="FLOAT", mode= "REQUIRED")
                       )))

# ----------------------------------------------------------
# 3. Weryfikacja tabel w ramach danego projektu i zbioru
# ----------------------------------------------------------

from google.cloud import bigquery

project = 'projekt-inwestycyjny'
location = 'europe-central2'
nazwa_datasetu = 'Dane_instrumentow'

# Utworzenie klienta
client = bigquery.Client(
    project = project,
    location = location)

# Weryfikacja tabel
tables = client.list_tables(
    bigquery.Dataset(project + "." + nazwa_datasetu))
for table in tables:
    print("Nazwa tabeli: " + table.table_id)
    print("Data utworzenia: " +  str(table.created))
    print("Rodzaj tabeli: " +  table.table_type + "\n")


# ----------------------------------------------------------
# 4. Utworzenie widoku w ramach danego projektu i zbioru
# ----------------------------------------------------------
    
from google.cloud import bigquery

project = 'projekt-inwestycyjny'
location = 'europe-central2'
nazwa_datasetu = 'Dane_instrumentow'
nazwa_widoku = 'widok'

# Utworzenie klienta
client = bigquery.Client(
    project = project,
    location = location)


query = f"""
    WITH 
    daily AS (SELECT * FROM `{project}.{nazwa_datasetu}.{nazwa_widoku}`),
    added_averages AS (
      SELECT
        *,
        ROUND(AVG(Close) OVER(PARTITION BY Ticker ORDER BY `Date` ROWS 4 PRECEDING), 2)       AS moving_average_5,
        ROUND(AVG(Close) OVER(PARTITION BY Ticker ORDER BY `Date` ROWS 14 PRECEDING), 2)      AS moving_average_15,
        ROUND(AVG(Close) OVER(PARTITION BY Ticker ORDER BY `Date` ROWS 69 PRECEDING), 2)      AS moving_average_70,
      FROM
        daily
        )
    SELECT * FROM added_averages
    """

# Utworzenie obiektu typu Table

view = bigquery.Table(
    table_ref = (project + "." + nazwa_datasetu + "." + nazwa_widoku))

# Przypisanie do atrybutu view_query wartosci query

view.view_query = query

# Utworzenie widoku

client.create_table(view)


# ----------------------------------------------------------
# 5. Odczyt danych z tabeli i przypisanie do DataFrame
# ----------------------------------------------------------

from google.cloud import bigquery

project_id = 'projekt-inwestycyjny'
dataset_id = 'Inflation'
table_id = 'Inflation'
destination_table = f"`{project_id}.{dataset_id}.{table_id}`"

query = f"""
    SELECT *
    FROM {destination_table}
    """

client = bigquery.Client()
query_job = client.query(query)
df = query_job.to_dataframe()

# ----------------------------------------------------------
# 5. Aktualizacja widoku w BigQuery.
# ----------------------------------------------------------


from google.cloud import bigquery

# Zainicjuj klienta BigQuery
client = bigquery.Client()

# Ustaw nazwę projektu, datasetu i widoku
project_id = ''
dataset_id = ''
view_id = ''

# Pobierz aktualną definicję widoku
view = client.get_table(table = f'{project_id}.{dataset_id}.{view_id}')

# Zmodyfikuj kod SQL w widoku
nowy_kod_sql = """
NEW SQL CODE
""".format(project_id, dataset_id)

view.view_query = nowy_kod_sql

# Zaktualizuj widok
client.update_table(view, ['view_query'])

print(f'Zaktualizowano kod SQL w widoku {project_id}.{dataset_id}.{view_id}.')
