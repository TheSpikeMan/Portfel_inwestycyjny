import yfinance as yf
import pandas as pd

# Lista tickerów spółek GPW
tickers = ['AMB.WA', 'XTB.WA']
all_data = pd.DataFrame()  # Stworzenie pustego DataFrame, do którego będziemy dodawać dane

for ticker in tickers:
    stock = yf.Ticker(ticker)

    # --- Dane dzienne: kurs zamknięcia, wolumen, obrót ---
    hist = stock.history(period="1d")
    hist['Turnover'] = hist['Close'] * hist['Volume']

    df_prices = hist[['Close', 'Volume', 'Turnover']].reset_index()
    df_prices = df_prices.melt(id_vars=['Date'], var_name='Metric', value_name='Value')
    df_prices['Ticker'] = ticker
    df_prices['Source'] = 'Price History'

    # --- Dywidendy ---
    div = stock.dividends.reset_index()
    df_div = div.rename(columns={'Dividends': 'Value'})
    df_div['Metric'] = 'Dividend'
    df_div['Ticker'] = ticker
    df_div['Source'] = 'Dividends'

    # --- Statystyki (info) ---
    info = stock.info
    df_info = pd.DataFrame(list(info.items()), columns=['Metric', 'Value'])
    df_info['Ticker'] = ticker
    df_info['Date'] = pd.to_datetime('today').normalize()
    df_info['Source'] = 'Info'
    df_info = df_info[['Ticker', 'Date', 'Metric', 'Value', 'Source']]

    # --- Dane finansowe roczne ---
    fin = stock.financials.T.reset_index().rename(columns={'index': 'Date'})
    fin = fin.melt(id_vars='Date', var_name='Metric', value_name='Value')
    fin['Ticker'] = ticker
    fin['Source'] = 'Financials Annual'

    # --- Dane finansowe kwartalne ---
    qfin = stock.quarterly_financials.T.reset_index().rename(columns={'index': 'Date'})
    qfin = qfin.melt(id_vars='Date', var_name='Metric', value_name='Value')
    qfin['Ticker'] = ticker
    qfin['Source'] = 'Financials Quarterly'

    # --- Przygotowanie do scalania ---
    df_div = df_div[['Ticker', 'Date', 'Metric', 'Value', 'Source']]
    df_prices = df_prices[['Ticker', 'Date', 'Metric', 'Value', 'Source']]
    fin['Date'] = pd.to_datetime(fin['Date'])
    qfin['Date'] = pd.to_datetime(qfin['Date'])

    # --- Scal dane ---
    temp_data = pd.concat([df_prices, df_div, df_info, fin, qfin], ignore_index=True)
    temp_data['Metric'] = temp_data['Metric'].astype(str)

    # --- Dodanie danych do głównego DataFrame ---
    all_data = pd.concat([all_data, temp_data], ignore_index=True)

# --- Eksport ---
all_data.to_csv('gpw_data.csv', index=False)

print("✅ Plik 'gpw_data.csv' został zapisany z danymi dla AMB.WA i XTB.WA.")