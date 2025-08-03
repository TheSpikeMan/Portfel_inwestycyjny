from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests
import os
from pathlib import Path
from dotenv import load_dotenv

# Pobieram zmienne środowiskowe
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Pobranie adresu URL ze zmiennej środowiskowej
CLOUD_RUN_URL = os.getenv("BQ_SEND_EMAIL_FROM_POST_REQUEST")

payload = {
    "to": "example@example.com",
    "subject": "Test Cloud Run",
    "body": "Wiadomość testowa"
}

def get_id_token(audience_url: str):
    return id_token.fetch_id_token(Request(), audience_url)

def call_send_email():
    token = get_id_token(CLOUD_RUN_URL)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    response = requests.post(CLOUD_RUN_URL, headers=headers, json=payload)
    print(response.status_code)
    print(response.text)

if __name__ == "__main__":
    call_send_email()