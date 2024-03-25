from PyQt6.QtWidgets import (
    QApplication, 
    QMainWindow, 
    QPushButton, 
    QGridLayout, 
    QWidget, 
    QDateEdit, 
    QLabel, 
    QComboBox, 
    QLineEdit,
    QCalendarWidget)
from PyQt6.QtCore import QSize, Qt, QDate
from PyQt6.QtGui import QFont
from google.cloud import bigquery
import pandas as pd
import time
import numpy as np


class BigQueryReaderAndExporter():
    
    def __init__(self):
        self.project = 'projekt-inwestycyjny'
        self.location = 'europe-central2'
        self.dataSetDaneIntrumentow = 'Dane_instrumentow'
        self.dataSetCurrencies      = 'Waluty'
        self.dataSetTransactions    = 'Transactions'
        self.dataSetInflation       = 'Inflation'
        self.tableDaily             = 'Daily'
        self.tableInstrumentTypes   = 'Instrument_types'
        self.tableInstruments       = 'Instruments'
        self.tableTreasuryBonds     = 'Treasury_Bonds'
        self.tableInflation         = 'Inflation'
        self.tableTransactions      = 'Transactions'
        self.tableCurrency          = 'Currency'
        self.viewTransactionsView   = 'Transactions_view'
        self.viewCurrencies         = 'Currency_view'
    
    def downloadDataFromBigQuery(self):

        client = bigquery.Client(project    = self.project,
                                 location   = self.location)
        
        # Downloading Currencies Data from Big Query view
        queryCurrencies = f"""
        SELECT
            Currency_date                                   AS Currency_date,
            Currency                                        AS Currency,
            Currency_close                                  AS Currency_close,
            Last_day_currency                               AS Last_day_currency
        FROM `{self.project}.{self.dataSetCurrencies}.{self.viewCurrencies}`
        """
        query_job_currencies = client.query(query=queryCurrencies)
        self.currenciesDataFrame = query_job_currencies.to_dataframe()

        # Downloading Instrument Types Data from Big Query
        queryInstrumentTypes = f"""
        SELECT
            Instrument_type_id   AS Instrument_type_id,
            Instrument_type      AS Instrument_type
        FROM `{self.project}.{self.dataSetDaneIntrumentow}.{self.tableInstrumentTypes}`
        """
        query_job_instrument_types = client.query(query=queryInstrumentTypes)
        self.instrumentTypesDataFrame = query_job_instrument_types.to_dataframe()

        # Download Instruments Data From Big Query
        queryInstruments = f"""
        SELECT *
        FROM `{self.project}.{self.dataSetDaneIntrumentow}.{self.tableInstruments}`
        ORDER BY Ticker ASC
        """
        query_job_instruments = client.query(query=queryInstruments)
        self.instrumentsDataFrame = query_job_instruments.to_dataframe()

        return self.currenciesDataFrame, self.instrumentTypesDataFrame, self.instrumentsDataFrame
    
    # Metoda służy wysłaniu danych do BigQuery
    def sendDataToBigQuery(self, data, destination):
        self.data          = data
        self.destination   = destination
        print("Wysyłam dane do BigQuery, cel: ", self.destination, ", dane: ", self.data)

class DodajInstrumentDoSlownika(QWidget):

    def __init__(self, instrumentTypesDataFrame, instrumentsDataFrame):
        super().__init__()
        self.instrumentTypesDataFrame = instrumentTypesDataFrame
        self.instrumentsDataFrame     = instrumentsDataFrame
        print("Dodaję instrument do słownika.")

        # Ustawienie parametrów okna oraz załadowanie widgetów
        self.setWindowTitle("Dodaj instrument")
        self.setFixedSize(QSize(800,500))
        self.addWidgets()

    def addWidgets(self):
        layout = QGridLayout()

        # Dodanie ComboBoxa do wyboru typu danych
        instrumentTypes = QComboBox()
        instrumentTypes.addItems(["Akcje", "ETF", "Obligacje skarbowe", "Obligacje korporacyjne"])
        layout.addWidget(instrumentTypes, 0, 0)

        # Dodanie przycisku do wysłania danych do BigQuery
        sendDataPushButton       = QPushButton()
        sendDataPushButton.setText("Wyślij dane do bazy")
        sendDataPushButton.pressed.connect(self.sendDataToBigQuery)
        layout.addWidget(sendDataPushButton, 1, 0)

        # Wyjście do poprzedniego okna
        returnButton             = QPushButton()
        returnButton.setText("Powrót")
        returnButton.pressed.connect(self.close)
        layout.addWidget(returnButton, 2, 0)

        self.setLayout(layout)
    
    # Metoda odpowiedzialna za wysłanie danych do BigQuery
    def sendDataToBigQuery(self):
        print("Wysyłam dane do BigQuer.")  

# Klasa obsługująca dodanie nowej transakcji.
class DodajTransakcje(QWidget):

    def __init__(self, 
                 currenciesDataFrame, 
                 instrumentTypesDataFrame,
                 instrumentsDataFrame):
        super().__init__()

        # Pobranie danych z klasy MainWindow (poprzez argumenty)
        self.currenciesDataFrame        = currenciesDataFrame
        self.instrumentTypesDataFrame   = instrumentTypesDataFrame
        self.instrumentsDataFrame       = instrumentsDataFrame

        # Ustawienie okna oraz załadowanie widgetów
        self.setWindowTitle("Transakcje")
        self.setFixedSize(QSize(800,500))
        self.addWidgets()
    
    def addWidgets(self):
        self.layout = QGridLayout()

        # Dodanie QLabel do opisu daty
        primaryLabel = QLabel()
        primaryLabel.setText("Transakcje")
        self.fontTitle = QFont()
        self.fontTitle.setPointSize(16)
        primaryLabel.setFont(self.fontTitle)
        self.layout.addWidget(primaryLabel, 0, 0, 1, 3, alignment= Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignHCenter)

        # Dodanie QLabel do opisu daty
        dateLabel = QLabel()
        dateLabel.setText("Data transakcji")
        self.layout.addWidget(dateLabel, 1, 0)

        # Dodanie QDateEdit do wyboru daty transakcji
        self.dateDateEdit = QDateEdit()
        self.dateDateEdit.setDisplayFormat("yyyy-MM-dd")
        self.dateDateEdit.setDate(QDate.currentDate())
        self.dateDateEdit.setEnabled(False)
        self.layout.addWidget(self.dateDateEdit, 1, 1)

        # Dodanie przycisku otwierającego kalendarz. Po naciśnięciu przycisku uruchamiana jest metoda
        # OpenCalendar, która tworzy nowy obiekt QCalendarWidget, pobiera od użytkownika datę,
        # wpisuje ją do pola obok i zamyka obiekt.
        self.openCalendarButton       = QPushButton()
        self.openCalendarButton  .setText("Kalendarz")
        self.openCalendarButton.pressed.connect(self.OpenCalendar)
        self.layout.addWidget(self.openCalendarButton, 1, 2)

        # Dodanie QLabel do opisu typu transakcji
        transactionTypeLabel = QLabel()
        transactionTypeLabel.setText("Typ transakcji")
        self.layout.addWidget(transactionTypeLabel, 2, 0)

        # Dodanie ComboBoxa do wyboru typu transakcji
        self.transactionsTypeComboBox = QComboBox()
        self.transactionsTypeComboBox.addItems(["Sprzedaż", "Zakup", "Wykup", "Dywidenda", "Odsetki"])
        self.transactionsTypeComboBox.currentTextChanged.connect(self.TransactionTypeBuyChosen)
        self.layout.addWidget(self.transactionsTypeComboBox, 2, 1)

        # Dodanie QLabel do opisu typu instrumentu finansowego
        instrumentTypeLabel = QLabel()
        instrumentTypeLabel.setText("Rodzaj instrumentu finansowego")
        self.layout.addWidget(instrumentTypeLabel, 3, 0)

        # Dodanie ComboBoxa do wyboru typu instrumentu finansowego
        self.instrumentTypeComboBox = QComboBox()
        self.instrumentTypeComboBox.addItems(self.instrumentTypesDataFrame['Instrument_type'].to_list())
        self.layout.addWidget(self.instrumentTypeComboBox, 3, 1)

        # Dodanie QLabel do opisu typu instrumentu finansowego
        instrumentLabel = QLabel()
        instrumentLabel.setText("Ticker")
        self.layout.addWidget(instrumentLabel, 4, 0)

        # Dodanie ComboBoxa do wyboru instrumentu finansowego
        self.instrumentComboBox = QComboBox()
        self.instrumentComboBox.addItems(self.instrumentsDataFrame['Ticker'].to_list())
        self.layout.addWidget(self.instrumentComboBox, 4, 1)

        # Dodanie QLabel do opisu ilości instrumentu podlegającego transakcji
        quantityLabel = QLabel()
        quantityLabel.setText("Ilość")
        self.layout.addWidget(quantityLabel, 5, 0)

        # Dodanie pola do wpisania ilości zakupionego waloru
        self.quantityLineEdit         = QLineEdit()
        self.layout.addWidget(self.quantityLineEdit, 5, 1)

        # Dodanie QLabel do opisu ceny instrumentu podlegającego transakcji
        priceLabel = QLabel()
        priceLabel.setText("Cena oraz waluta")
        self.layout.addWidget(priceLabel, 6, 0)

        # Dodanie pola do ceny zakupionego waloru
        self.priceLineEdit            = QLineEdit()
        self.layout.addWidget(self.priceLineEdit, 6, 1)

        # Dodanie ComboBoxa do wyboru waluty instrumentu finansowego
        self.currencyComboBox = QComboBox()
        self.currencyComboBox.addItems(["PLN", "USD", "EUR"])
        self.currencyComboBox.setCurrentText("PLN")
        # Ustawiam walutę na domyślną w razie braku zmian waluty przed datą. Dane wykorzystywane przez metodę DateChanged
        self.currentCurrency = "PLN"
        self.currencyComboBox.currentTextChanged.connect(self.CurrencyChanged)
        self.layout.addWidget(self.currencyComboBox, 6, 2)

        # Dodanie QLabel do opisu kursu waluty instrumentu podlegającego transakcji
        currencyValueLabel = QLabel()
        currencyValueLabel.setText("Kurs waluty")
        self.layout.addWidget(currencyValueLabel, 7, 0)

        # Dodanie pola do kursu waluty waloru
        self.currencyValueLineEdit           = QLineEdit()
        self.currencyValueLineEdit.setText("1")
        self.layout.addWidget(self.currencyValueLineEdit, 7, 1)

        # Dodanie QLabel do opisu prowizji instrumentu podlegającego transakcji
        commisionLabel = QLabel()
        commisionLabel.setText("Prowizja")
        self.layout.addWidget(commisionLabel, 8, 0)

        # Dodanie pola do prowizji zakupionego waloru
        self.commisionLineEdit          = QLineEdit()
        self.layout.addWidget(self.commisionLineEdit, 8, 1)

        # Dodanie QLabel do opisu wartośći instrumentu podlegającego transakcji
        valueLabel = QLabel()
        valueLabel.setText("Wartość")
        self.layout.addWidget(valueLabel, 9, 0)

        # Dodanie pola do wpisania wartości
        self.valueLineEdit              = QLineEdit()
        self.layout.addWidget(self.valueLineEdit, 9, 1)

        # Dodanie przycisku do przeliczenia wartości
        self.valueCalculateButton       = QPushButton("Przelicz wartość")
        self.valueCalculateButton.pressed.connect(self.CalculateValue)
        self.layout.addWidget(self.valueCalculateButton)

        # Dodanie QLabel do opisu podatku
        taxLabel = QLabel()
        taxLabel.setText("Czy zapłacono podatek?")
        self.layout.addWidget(taxLabel, 10, 0)

        # Dodanie ComboBoxa do wyboru waluty instrumentu finansowego
        self.taxComboBox = QComboBox()
        self.taxComboBox.addItems(["Tak", "Nie"])
        self.taxComboBox.setCurrentText("Tak")
        self.layout.addWidget(self.taxComboBox, 10, 1)
        self.taxComboBox.currentTextChanged.connect(self.taxStateChosen)

        # Dodanie QLabel do opisu wartości podatku
        taxValueLabel = QLabel()
        taxValueLabel.setText("Wartość podatku")
        self.layout.addWidget(taxValueLabel, 11, 0)

        # Dodanie pola do wpisania wartości podatku
        self.taxValueLineEdit              = QLineEdit()
        self.layout.addWidget(self.taxValueLineEdit, 11, 1)

        # Dodanie przycisku do wysłania danych do BigQuery
        sendDataPushButton       = QPushButton()
        sendDataPushButton.setText("Wyślij dane do bazy")
        sendDataPushButton.pressed.connect(self.sendDataToBigQuery)
        self.layout.addWidget(sendDataPushButton, 12, 1)

        # Wyjście do poprzedniego okna
        returnButton             = QPushButton()
        returnButton.setText("Powrót")
        returnButton.pressed.connect(self.close)
        self.layout.addWidget(returnButton, 13, 1)

        self.layout.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignHCenter)
        self.layout.setSpacing(10)
        self.layout.setContentsMargins(150,20,150,20)
        self.layout.setColumnStretch(0, 1)
        self.layout.setColumnStretch(1, 2)
        self.layout.setColumnStretch(2, 1)
        self.setLayout(self.layout)
    
    # Metoda sprawdza aktualny stan ComboBoxa i w zależności od niego definiuje widoczność lub nie pola 'self.taxValueLineEdit'
    def taxStateChosen(self, currentTextChanged):
        print("Zmieniono stan")
        print("Obecny stan: ", currentTextChanged)
        if currentTextChanged == "Nie":
            self.taxValueLineEdit.setEnabled(False)
        else:
            self.taxValueLineEdit.setEnabled(True)

    # Metoda OpenCalendar tworzy obiekt QCalendarWidget i uruchamia metodę DateChanged, która pobiera datę z kalendarza
    # i ustawia ją w polu bok. Na końcu obiekt jest zamykany.
    def OpenCalendar(self):
        self.calendarWidget = QCalendarWidget()
        self.calendarWidget.selectionChanged.connect(self.DateChanged)
        self.layout.addWidget(self.calendarWidget, 1, 2, 7, 10)

    # Przekazuję wybraną datę do pola tekstowego
    def DateChanged(self):
        #selected_date = self.calendarWidget.selectedDate().toString("yyyy-MM-dd")
        self.selected_date = self.calendarWidget.selectedDate()
        self.dateDateEdit.setDate(self.selected_date)

        # Jeżeli waluta była zmieniana jako argument podpinana jest z metody CurrencyChanged. Jeżeli nie, z domyślnego
        # parametru określonego w definicji pola.
        self.CurrencyChanged(self.currentCurrency)
        self.calendarWidget.close()
        self.layout.setContentsMargins(150,20,150,20)

    def CurrencyChanged(self, currentTextChanged):
        # Pobranie aktualnej daty
        self.dateEditField = self.dateDateEdit.date()
        self.dateEditField = self.dateEditField.toString("yyyy-MM-dd")
        # Pobranie waluty z Comboboxa
        self.currentCurrency  = currentTextChanged
        print("Metoda CurrencChanged")

        # Sprawdzenie waluty i podpięcie ostatniego kursu waluty dla danego dnia. Wykorzystanie widoku Currency_view.
        if self.currentCurrency != 'PLN':
            self.lastDayCurrency = self.currenciesDataFrame.query(f"Currency== '{self.currentCurrency}' \
                                                                  & Currency_date== '{self.dateEditField}'") \
                                                                    ['Last_day_currency'].\
                                                                    iloc[0]
            self.currencyValueLineEdit.setText(f"{self.lastDayCurrency}")
        else:
            self.currencyValueLineEdit.setText("1")
        

    # Metoda oblicza wartość na podstawie ilości oraz ceny
    def CalculateValue(self):
        if self.priceLineEdit.text() != "" and self.quantityLineEdit.text() != "":
            value = str(
                        round(
                            float(self.priceLineEdit.text()) * \
                                float(self.quantityLineEdit.text()) * \
                                    float(self.currencyValueLineEdit.text()), 
                        2))
            self.valueLineEdit.setText(value)
        else:
            pass    

    def TransactionTypeBuyChosen(self, currentTextChanged):
        self.transactionType    = currentTextChanged
        if self.transactionType == "Zakup":
            self.taxComboBox.setEnabled(False)
            self.taxValueLineEdit.setEnabled(False)
        else:
            self.taxComboBox.setEnabled(True)
            self.taxValueLineEdit.setEnabled(True)

    def PrepareDataForBigQueryExport(self):

        print("Uruchamiam funkcję PrepareDataForBigQueryExport")

        # Przypisanie wartości domyślnych do zmiennych
        self.Transaction_id        = np.nan
        self.Transaction_date      = np.nan
        self.Transaction_type      = np.nan
        self.Currency              = np.nan
        self.Transaction_price     = np.nan
        self.Transaction_amount    = np.nan
        self.Instrument_id         = np.nan
        self.Commision_id          = np.nan
        self.Dirty_bond_price      = np.nan
        self.Tax_paid              = np.nan
        self.Tax_value             = np.nan

        # Przypisanie wartości do zmiennych

        # Do podpięcia transaction_id - SQL
        #self.Transaction_id        = 
        self.Transaction_date      = self.dateDateEdit.date().toString("yyyy-MM-dd")
        self.Transaction_type      = self.transactionsTypeComboBox.currentText()

        if self.Transaction_type   == "Sprzedaż":
            self.Transaction_type  = "Sell"
        elif self.Transaction_type == "Zakup":
            self.Transaction_type  = "Buy"
        else:
            self.Transaction_Type  = np.nan
        
        self.Currency              = self.currencyComboBox.currentText()
        self.Transaction_price     = self.priceLineEdit.text()
        self.Transaction_amount    = self.quantityLineEdit.text()
        
        # Do podpięcia instrument ID - SQL
        # self.Instrument_id
        self.Commision_id          = self.commisionLineEdit.text()
        # self.Dirty_bond_price
        self.Tax_paid              = self.taxComboBox.currentText()
        self.Tax_value             = self.taxValueLineEdit.text()

        transaction_parameters = [self.Transaction_id,
                                  self.Transaction_date,
                                  self.Transaction_type,
                                  self.Currency,
                                  self.Transaction_price,
                                  self.Transaction_amount,
                                  self.Instrument_id,
                                  self.Commision_id,
                                  self.Dirty_bond_price,
                                  self.Tax_paid,
                                  self.Tax_value
                                  ]
        
        transaction_parameters_dataFrame = pd.DataFrame(data=transaction_parameters)
        return transaction_parameters_dataFrame
        
    # Metoda odpowiedzialna za wysłanie danych do BigQuery
    def sendDataToBigQuery(self):
        print("Wysyłam dane do BigQuery.")
        transaction_data_to_export = self.PrepareDataForBigQueryExport()

        # Tworzę obiekt BigQueryReaderAndExporter do eksportu danych do BQ
        bigQueryExporterObject = BigQueryReaderAndExporter()
        bigQueryExporterObject.sendDataToBigQuery(transaction_data_to_export, "Testowa destynacja")

# Klasa tymczsowo nieaktywna - do wprowadzenia w przyszłości
class InitialWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.title = "Pobieranie danych"
        self.setWindowTitle(self.title)
        self.setFixedSize(QSize(400,250))
        self.addWidgets()
    
    def addWidgets(self):
        self.layout = QGridLayout()
        welcomeLabel = QLabel()
        welcomeLabel.setText("Trwa pobieranie danych. Proszę czekać.")
        self.layout.addWidget(welcomeLabel, 0, 0, alignment= Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignHCenter)
        centralWidget = QWidget()
        centralWidget.setLayout(self.layout)
        self.setCentralWidget(centralWidget)
        

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.title = "Portfel_inwestycyjny_Desktop_App"
        # Ustawienie parametrów okna i ustawienie widgetów
        self.setWindowTitle(self.title)
        self.setFixedSize(QSize(800,500))

        # Utworzenie obiektu klasy BigQueryReaderAndExporter, wywołanie na nim metody 'downloadDataFromBigQuery' - pobranie 
        # danych z BigQuery, a następnie przypisanie wyniku pracy metody do zmiennych
        # Tą część będzie można ulepszyć - w tej chwili pobieranie danych powoduje zwiększenie czasu uruchamiania programu.
        # Warto będzie dodać okno wstępne z informacją o konieczności pobrania danych i oczekiwania.
        self.currenciesDataFrame, self.instrumentTypesDataFrame, self.instrumentsDataFrame = \
            BigQueryReaderAndExporter().downloadDataFromBigQuery()
        self.addWidgets()
    
    def addWidgets(self):
        layout = QGridLayout()

        # Dodanie przycisku odpowiedzialnego za autoryzację
        self.authorizeButton = QPushButton("Autoryzuj")
        #self.authorizeButton.clicked.connect(self.addTransactions)
        layout.addWidget(self.authorizeButton, 0, 0)

        # Dodanie przycisku odpowiedzialnego za dodanie transakcji
        self.addTransaction = QPushButton("Dodaj transakcję")
        self.addTransaction.clicked.connect(self.addTransactions)
        layout.addWidget(self.addTransaction, 1, 0)

        # Dodanie przycisku odpowiedzialnego za dodanie nowego instrumentu do słownika
        self.addInstr = QPushButton("Dodaj nowy instrument do słownika")
        self.addInstr.clicked.connect(self.addInstrument)
        layout.addWidget(self.addInstr, 2, 0)

        # Dodanie przycisku odpowiedzialnego za zamknięcie okna głównego
        self.closeWindow = QPushButton("Zamknij")
        self.closeWindow.clicked.connect(self.close)
        layout.addWidget(self.closeWindow, 3, 0)

        layout.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignHCenter)
        layout.setSpacing(10)
        layout.setContentsMargins(100,20,100,20)

        # Zdefiniowanie głównego widgetu
        centralWidget = QWidget()
        centralWidget.setLayout(layout)
        self.setCentralWidget(centralWidget)
    
    # Zdefiniowanie metody uruchamianej po naciśnięciu przycisku 'AddTransaction'
    def addTransactions(self):
        self.dodajTransakcje = DodajTransakcje(self.currenciesDataFrame, 
                                               self.instrumentTypesDataFrame,
                                               self.instrumentsDataFrame)
        self.dodajTransakcje.show()
    
    # Zdefiniowane metody uruchamianej po naciśnięciu przycisku 'AddInstr'
    def addInstrument(self):
        self.addInstrument = DodajInstrumentDoSlownika(self.instrumentTypesDataFrame,
                                                       self.instrumentsDataFrame)
        self.addInstrument.show()
        
# Main part of the app
app = QApplication([]) 

# Inicjalizacje nieaktywne - do aktywacji podczas uruchamiania okna początkowego.
# window = InitialWindow()
# window.show()
window = MainWindow()
window.show()

app.exec()

