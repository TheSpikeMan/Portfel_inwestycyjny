
"""
Kod wykorzystuje bibliotekę request do wysłania zapytania POST
na serwer uruchamiany w celu wysłania emaila.
Żadanie wysyłane jest na adres 'send-email' z podaniem całej treści emaila w bieżącym kodzie.

Do uruchomienia kodu niezbędne jest uruchomienie serwera na porcie 5000.

"""

import requests

url = "http://localhost:5000/send-email"  # albo adres serwera, gdzie jest Flask

data = {
    "to": "example@example.com",
    "subject": "Testowy temat",
    "body": "Halo Halo!"
}

response = requests.post(url, json=data)

print("Status:", response.status_code)
print("Odpowiedź JSON:", response.json())