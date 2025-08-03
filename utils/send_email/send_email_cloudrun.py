import os
import base64
from flask import Flask, request, jsonify
from email.message import EmailMessage
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

app = Flask(__name__)

# Ścieżki do plików
TOKEN_PATH = '/secrets/Gmail_token.json'
CREDENTIALS_PATH = '/secrets/OAuth2_Credentials.json'
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

def load_credentials():
    creds = None

    # Wczytaj token z pliku
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    # Odśwież token, jeśli wygasł
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return creds

def send_email(recipient, subject, body):
    creds = load_credentials()
    if not creds or not creds.valid:
        raise Exception("Nieprawidłowe lub brakujące poświadczenia")

    service = build('gmail', 'v1', credentials=creds)
    
    message = EmailMessage()
    message.set_content(body)
    message['To'] = recipient
    message['From'] = "me"
    message['Subject'] = subject

    # Zakoduj wiadomość do formatu wymaganym przez Gmail API
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

    create_message = {
        'raw': encoded_message
    }

    send_message = service.users().messages().send(userId="me", body=create_message).execute()
    return send_message

@app.route('/send-email', methods=['POST'])
def trigger_email():
    data = request.get_json()
    recipient = data.get('to')
    subject = data.get('subject')
    body = data.get('body')

    if not all([recipient, subject, body]):
        return jsonify({"error": "Brakuje danych"}), 400

    result = send_email(recipient, subject, body)
    return jsonify(result), 200