import os, base64

# Klasa pythonowa do tworzenia i zarządzania wiadomościami email
from email.message import EmailMessage

# Biblioteki do autoryzacji OAuth2.0
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Biblioteka do budowy klienta GmailAPI
from googleapiclient.discovery import build

# Bibilioteki do odczytu pliku .env
from dotenv import load_dotenv
from pathlib import Path

# Zakres działania aplikacji - wysyłka emaili
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

# Pobieram zmienne środowiskowe
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Autoryzacja
def get_gmail_service():
    creds = None
    # Jeśli token już istnieje (jestem zalogowany) załaduj token
    if os.path.exists('token.json'):
        # Ładowanie tokena z pliku - tworzy instancję klasy 'Credentials' na bazie pliku z tokenem i zakresu
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # Jeżeli token nie istnieje lub jest nieważny odśwież go
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # Odświeżenie tokena jeśli jest nieważny.
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials, SCOPES)
            # Autoryzacja w oknie przeglądarki do konkretnego zakresu zdefiniowanego w "SCOPES"
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            # Zapis tokena do pliku .json
            token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

service = get_gmail_service()

# Wysyłka emaili
def send_email(to, subject, body_text):
    try:
        message = EmailMessage()
        message.set_content(body_text)
        message['To'] = to
        message['From'] = 'me'
        message['Subject'] = subject

        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = { 'raw': encoded_message }

        send_message = service.users().messages().send(userId="me", body=create_message).execute()
        print(f"Wiadomość wysłana! ID: {send_message['id']}")
    except Exception as e:
        print(f"❌ Błąd: {e}")

# Test
send_email(
    to="adres@gmail.com",
    subject="Test Gmail API",
    body_text="Działa! ✨"
)
