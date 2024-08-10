"""
project_name     --> Nazwa projektu
project_set_name --> Nazwa zbioru danych
table_name       --> Nazwa tabeli
"""


SELECT ddl
FROM `{project_name}.{project_set_name}.INFORMATION_SCHEMA.TABLES`
WHERE table_name = '{table_name}';