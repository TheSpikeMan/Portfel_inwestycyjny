from google_auth_oauthlib.flow import InstalledAppFlow
import os

SCOPES = ['https://www.googleapis.com/auth/gmail.send']
TOKEN_PATH = 'secrets/Gmail_token.json'
CREDENTIALS_PATH = 'secrets/OAuth2_Credentials.json'

def authorize():
    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
    creds = flow.run_local_server(port=0)
    os.makedirs(os.path.dirname(TOKEN_PATH), exist_ok=True)
    with open(TOKEN_PATH, 'w') as token_file:
        token_file.write(creds.to_json())
    print("Token zapisany do", TOKEN_PATH)

if __name__ == '__main__':
    authorize()