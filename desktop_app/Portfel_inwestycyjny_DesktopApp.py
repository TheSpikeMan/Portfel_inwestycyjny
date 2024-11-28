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
    QCalendarWidget,
    QTextEdit)
from PyQt6.QtCore import QSize, Qt, QDate, QEvent, QThread, pyqtSignal
from PyQt6.QtGui import QFont
from google.cloud import bigquery
import pandas as pd
import numpy as np


class WorkerThread(QThread):
    finished = pyqtSignal()
    data_ready = pyqtSignal(object, object, object)

    def __init__(self, bqrae):
        super().__init__()
        self.bqrae = bqrae

    def run(self):
        # Przykład operacji w tle (np. pobieranie danych, długotrwała operacja)
        print("Rozpoczynam operację w tle...")
        self.currenciesDataFrame, self.instrumentsDataFrame, self.transactionsDataFrame = self.bqrae.downloadDataFromBigQuery()  # Wykonanie metody
        print("Operacja zakończona!")

        self.data_ready.emit(self.currenciesDataFrame, self.instrumentsDataFrame, self.transactionsDataFrame)
        self.finished.emit()

class BigQueryProject():

    def __init__(self,
                 location,
                 dataSetDaneIntrumentow,
                 dataSetCurrencies,
                 dataSetTransactions,
                 dataSetInflation,
                 tableDaily,
                 tableInstrumentTypes,
                 tableInstruments,
                 tableTreasuryBonds,
                 tableInflation,
                 tableTransactions,
                 tableCurrency,
                 viewTransactionsView,
                 viewCurrencies):
        self.location               = location
        self.dataSetDaneIntrumentow = dataSetDaneIntrumentow
        self.dataSetCurrencies      = dataSetCurrencies
        self.dataSetTransactions    = dataSetTransactions
        self.dataSetInflation       = dataSetInflation
        self.tableDaily             = tableDaily
        self.tableInstrumentTypes   = tableInstrumentTypes
        self.tableInstruments       = tableInstruments
        self.tableTreasuryBonds     = tableTreasuryBonds
        self.tableInflation         = tableInflation
        self.tableTransactions      = tableTransactions
        self.tableCurrency          = tableCurrency
        self.viewTransactionsView   = viewTransactionsView
        self.viewCurrencies         = viewCurrencies

class BigQueryReaderAndExporter():
    
    def __init__(self, project, bigQueryProjectObject):
        self.project                = project
        self.location               = bigQueryProjectObject.location
        self.dataSetDaneIntrumentow = bigQueryProjectObject.dataSetDaneIntrumentow
        self.dataSetCurrencies      = bigQueryProjectObject.dataSetCurrencies
        self.dataSetTransactions    = bigQueryProjectObject.dataSetTransactions
        self.dataSetInflation       = bigQueryProjectObject.dataSetInflation
        self.tableDaily             = bigQueryProjectObject.tableDaily
        self.tableInstrumentTypes   = bigQueryProjectObject.tableInstrumentTypes
        self.tableInstruments       = bigQueryProjectObject.tableInstruments
        self.tableTreasuryBonds     = bigQueryProjectObject.tableTreasuryBonds
        self.tableInflation         = bigQueryProjectObject.tableInflation
        self.tableTransactions      = bigQueryProjectObject.tableTransactions
        self.tableCurrency          = bigQueryProjectObject.tableCurrency
        self.viewTransactionsView   = bigQueryProjectObject.viewTransactionsView
        self.viewCurrencies         = bigQueryProjectObject.viewCurrencies

    
    def downloadLastTransactionId(self):
        client = bigquery.Client(project      = self.project,
                                location      = self.location)
        
        # Download last transaction id from BigQuery
        queryMaxTransactionId = f"""
        SELECT
            MAX(Transaction_id)  AS Max_transaction_id
        FROM `{self.project}.{self.dataSetTransactions}.{self.tableTransactions}`
        """
        query_job_max_transaction_id = client.query(query=queryMaxTransactionId)
        self.maxTransactionId = query_job_max_transaction_id.to_dataframe()

        return self.maxTransactionId
    
    def downloadDataFromBigQuery(self):

        client = bigquery.Client(project    = self.project,
                                 location   = self.location)
        
        # Downloading Currencies Data from Big Query view
        queryCurrencies = f"""
        SELECT
            Currency_date                                   AS Currency_date,
            Currency                                        AS Currency,
            Currency_close                                  AS Currency_close,
            last_currency_close                             AS last_currency_close
        FROM `{self.project}.{self.dataSetCurrencies}.{self.viewCurrencies}`
        """
        query_job_currencies = client.query(query=queryCurrencies)
        self.currenciesDataFrame = query_job_currencies.to_dataframe()

        # Downloading Instruments and instrumen Types Data from Big Query
   
        query_Instrument_And_Instrument_Types = f"""
        WITH
        instruments                   AS (SELECT * FROM `{self.project}.{self.dataSetDaneIntrumentow}.{self.tableInstruments}`),
        instrument_types              AS (SELECT * FROM `{self.project}.{self.dataSetDaneIntrumentow}.{self.tableInstrumentTypes}`)

        SELECT
            instruments.Instrument_id         AS Instrument_id,
            instruments.Ticker                AS Ticker,
            instruments.Status                AS Status,
            instrument_types.Instrument_type  AS Instrument_type
        FROM instruments
        LEFT JOIN instrument_types
        ON instruments.Instrument_type_id   = instrument_types.Instrument_type_id
        ORDER BY Ticker ASC
        """

        query_job_instruments = client.query(query=query_Instrument_And_Instrument_Types)
        self.instrumentsDataFrame = query_job_instruments.to_dataframe()

        # downloading transactions data
        query_transactions = f"""
        SELECT
            instruments.instrument_id                           AS Ticker_id,
            instruments.Ticker                              AS Ticker,
            transactions.transaction_date_ticker_amount     AS Amount
        FROM `{self.project}.{self.dataSetTransactions}.{self.viewTransactionsView}` AS transactions
        LEFT JOIN `{self.project}.{self.dataSetDaneIntrumentow}.{self.tableInstruments}` AS instruments
        ON transactions.Instrument_id = instruments.Instrument_id
        WHERE instruments.status = 1
        QUALIFY 
        TRUE
        AND ROW_NUMBER() OVER last_transaction_date_amount = 1
        WINDOW
        last_transaction_date_amount AS (
            PARTITION BY Ticker
            ORDER BY Transaction_date DESC
        )
        ORDER BY
        Ticker
        """
        query_job_transactions = client.query(query=query_transactions)
        self.transactionsDataFrame = query_job_transactions.to_dataframe()

        return self.currenciesDataFrame, self.instrumentsDataFrame, self.transactionsDataFrame

    
    # Metoda służy wysłaniu danych do BigQuery
    def sendDataToBigQuery(self, data, destination):
        self.data               = data
        self.destination        = destination
        print("Wysyłam dane do BigQuery, cel: ", self.destination)

        client = bigquery.Client()

        if self.destination     == "Dane transakcyjne":
            self.destination    = f"{self.project}.{self.dataSetTransactions}.{self.tableTransactions}"
            self.schema = [bigquery.SchemaField(name = 'Transaction_id', field_type = "INTEGER",
                                        mode = "REQUIRED"),
                           bigquery.SchemaField(name = 'Transaction_date', field_type = "DATE",
                                        mode = "REQUIRED"),
                           bigquery.SchemaField(name = 'Transaction_type', field_type = "STRING",
                                        mode = "REQUIRED"),
                           bigquery.SchemaField(name = 'Currency', field_type = "STRING",
                                        mode = "REQUIRED"),
                           bigquery.SchemaField(name = 'Transaction_price', field_type = "FLOAT",
                                        mode = "NULLABLE"),
                           bigquery.SchemaField(name = 'Transaction_amount', field_type = "FLOAT",
                                        mode = "REQUIRED"),
                           bigquery.SchemaField(name = 'Instrument_id', field_type = "INTEGER",
                                        mode = "REQUIRED"),
                           bigquery.SchemaField(name = 'Commision_id', field_type = "FLOAT",
                                        mode = "NULLABLE"),
                           bigquery.SchemaField(name = 'Tax_paid', field_type = "BOOLEAN",
                                        mode = "REQUIRED"),
                           bigquery.SchemaField(name = 'Tax_value', field_type = "FLOAT",
                                        mode = "NULLABLE")
                                        ]
        else:
            self.informationTextEdit.append("Brak zdefiniowanej schemy dla tego przypadku")
            
        
        job_config = bigquery.LoadJobConfig(schema = self.schema,
                                    write_disposition = "WRITE_APPEND")
        
        try:
            job = client.load_table_from_dataframe(self.data, 
                                                   self.destination,
                                                   job_config = job_config)
            job.result()
            
            self.message = "Dane zostały wyeksportowane do tabeli w BigQuery"
        except Exception as e:
            self.message = f"Error uploading data to BigQuery: {str(e)}"
        return self.message

class DodajInstrumentDoSlownika(QWidget):

    def __init__(self, instrumentsDataFrame):
        super().__init__()
        self.instrumentsDataFrame     = instrumentsDataFrame
        print("Dodaję instrument do słownika.")

        # Ustawienie parametrów okna oraz załadowanie widgetów
        self.setWindowTitle("Dodaj instrument")
        self.setFixedSize(QSize(800,500))
        self.addWidgets()

    def addWidgets(self):
        self.layout = QGridLayout()

        # Dodanie QLabel do opisu daty
        primaryLabel = QLabel()
        primaryLabel.setText("Słownik instrumentów")
        self.fontTitle = QFont()
        self.fontTitle.setPointSize(16)
        primaryLabel.setFont(self.fontTitle)
        self.layout.addWidget(primaryLabel, 0, 0, 1, 3,
        alignment=Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignHCenter)

        # Dodanie QLabel do rodzaju instrumentu finansowego
        self.instrumentTypeLabel = QLabel()
        self.instrumentTypeLabel.setText("Rodzaj instrumentu finansowego")
        self.layout.addWidget(self.instrumentTypeLabel, 2, 0)

        # Dodanie ComboBoxa do wyboru typu danych
        self.instrumentTypes = QComboBox()
        self.instrumentTypes.addItems(["Akcje", "ETF", "Obligacje skarbowe", "Obligacje korporacyjne"])
        self.layout.addWidget(self.instrumentTypes, 2, 1)

        # Dodanie QLabel wskazującego na konkretny ticker
        self.instrumentLabel = QLabel()
        self.instrumentLabel.setText("Ticker")
        self.layout.addWidget(self.instrumentLabel, 3, 0)

        # Dodanie QLineEdit do wprowadzenia Tickera
        self.tickerLineEdit = QLineEdit()
        # self.quantityLineEdit.installEventFilter(self) --> Do wprowadzenia
        self.layout.addWidget(self.tickerLineEdit, 3, 1)

        # Dodanie QLabel wskazującego na konkretną nazwę instrumentu
        self.instrumentNameLabel = QLabel()
        self.instrumentNameLabel.setText("Pełna nazwa instrumentu")
        self.layout.addWidget(self.instrumentNameLabel, 4, 0)

        # Dodanie QLineEdit do wprowadzenia nazwy instrumentu
        self.instrumentNameLineEdit = QLineEdit()
        self.layout.addWidget(self.instrumentNameLineEdit, 4, 1)

        # Dodanie QLabel wskazującego na konkretną jednostkę danego instrumentu
        self.instrumentUnitLabel = QLabel()
        self.instrumentUnitLabel.setText("Jednostka")
        self.layout.addWidget(self.instrumentUnitLabel, 5, 0)

        # Dodanie QComboBox do wprowadzenia jednostki instrumentu
        self.instrumentUnitComboBox = QComboBox()
        self.instrumentUnitComboBox.addItems(["1", "10", "100", "1000", "10000"])
        self.layout.addWidget(self.instrumentUnitComboBox, 5, 1)

        # Dodanie QLabel wskazującego na kraj notowania danego instrumentu
        self.countryLabel = QLabel()
        self.countryLabel.setText("Kraj notowania (skrót)")
        self.layout.addWidget(self.countryLabel, 6, 0)

        # Dodanie QLineEdit do wprowadzenia kraju notowania danego instrumentu
        self.countryLineEdit = QLineEdit()
        self.layout.addWidget(self.countryLineEdit, 6, 1)

        # Dodanie QLabel wskazującego na identyfikator rynku danego instrumentu
        self.marketLabel = QLabel()
        self.marketLabel .setText("Identyfiktor giełdy (skrót)")
        self.layout.addWidget(self.marketLabel, 7, 0)

        # Dodanie QLineEdit do wprowadzenia identyfikatora rynku danego instrumentu
        self.marketLineEdit = QLineEdit()
        self.layout.addWidget(self.marketLineEdit, 7, 1)

        # Dodanie QLabel wskazującego na walutę w jakiej notowany jest dany instrument
        self.currencyLabel = QLabel()
        self.currencyLabel .setText("Waluta")
        self.layout.addWidget(self.currencyLabel, 8, 0)

        # Dodanie QComboBoc do wprowadzenia waluty w jakiej notowany jest dany instrument
        self.currencyComboBox = QComboBox()
        self.currencyComboBox.addItems(["USD", "EUR", "GBP", "CHF", "PLN"])
        self.layout.addWidget(self.currencyComboBox, 8, 1)

        # Dodanie QLabel wskazującego na politykę dystrybucji danego instrumentu
        self.distributionPolicyLabel = QLabel()
        self.distributionPolicyLabel.setText("Polityka dystrybucji")
        self.layout.addWidget(self.distributionPolicyLabel, 9, 0)

        # Dodanie QComboBox do wprowadzenia polityki dystrybucji danego instrumentu
        self.distributionPolicyCombobox = QComboBox()
        self.distributionPolicyCombobox.addItems(["Distributing", "Accumulating"])
        self.layout.addWidget(self.distributionPolicyCombobox, 9, 1)

        # Dodanie QLabel wskazującego na typ instrumentu
        self.instrumentTypeIdLabel = QLabel()
        self.instrumentTypeIdLabel.setText("Identyfikator typu instrumentu")
        self.layout.addWidget(self.instrumentTypeIdLabel, 10, 0)

        # Dodanie QLineEdit do wprowadzenia typu danego instrumentu
        self.instrumentTypeIdLineEdit = QLineEdit()
        self.layout.addWidget(self.instrumentTypeIdLineEdit, 10, 1)

        # Dodanie QLabel wskazującego na siedzibę danego instrumentu
        self.instrumentHeadquarterLabel = QLabel()
        self.instrumentHeadquarterLabel.setText("Siedziba")
        self.layout.addWidget(self.instrumentHeadquarterLabel, 11, 0)

        # Dodanie QLineEdit do wprowadzenia siedziby danego instrumentu
        self.instrumentHeadquarterLineEdit = QLineEdit()
        self.layout.addWidget(self.instrumentHeadquarterLineEdit, 11, 1)

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
    
    # Metoda odpowiedzialna za wysłanie danych do BigQuery
    def sendDataToBigQuery(self):
        print("Wysyłam dane do BigQuer.")  

# Klasa obsługująca dodanie nowej transakcji.
class DodajTransakcje(QWidget):

    def __init__(self,
                 project_name,
                 bqrae,
                 currenciesDataFrame,
                 instrumentsDataFrame,
                 transactionsDataFrame,
                 maxTransactionId):
        super().__init__()

        # Pobranie danych z klasy MainWindow (poprzez argumenty)
        self.currenciesDataFrame        = currenciesDataFrame
        self.instrumentsDataFrame       = instrumentsDataFrame
        self.transactionsDataFrame      = transactionsDataFrame

        # Pobranie obiektu klasy BigQueryProject i nazwy projektu
        self.project                    = project_name
        self.bqrae                      = bqrae

        # Konwersja typu DataFrame na float
        self.maxTransactionId           = maxTransactionId.iloc[0,0]

        # Ustawienie okna oraz załadowanie widgetów
        self.setWindowTitle("Transakcje")
        self.setFixedSize(QSize(800,600))
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
        self.openCalendarButton.setText("Kalendarz")
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
        self.instrumentTypeComboBox.addItems(set(self.instrumentsDataFrame['Instrument_type'].to_list()))
        self.instrumentTypeComboBox.currentTextChanged.connect(self.instrumentTypeChanged)
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
        self.quantityLineEdit.installEventFilter(self)
        self.layout.addWidget(self.quantityLineEdit, 5, 1)

        # Dodanie QLabel do opisu ceny instrumentu podlegającego transakcji
        priceLabel = QLabel()
        priceLabel.setText("Cena oraz waluta")
        self.layout.addWidget(priceLabel, 6, 0)

        # Dodanie pola do ceny zakupionego waloru
        self.priceLineEdit            = QLineEdit()
        self.priceLineEdit.installEventFilter(self)
        self.dot_entered = False
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
        self.currencyValueLineEdit.setEnabled(False)
        self.layout.addWidget(self.currencyValueLineEdit, 7, 1)

        # Dodanie QLabel do opisu prowizji instrumentu podlegającego transakcji
        commisionLabel = QLabel()
        commisionLabel.setText("Prowizja")
        self.layout.addWidget(commisionLabel, 8, 0)

        # Dodanie pola do prowizji zakupionego waloru
        self.commisionLineEdit          = QLineEdit()
        self.commisionLineEdit.installEventFilter(self)
        self.layout.addWidget(self.commisionLineEdit, 8, 1)

        # Dodanie QLabel do opisu wartośći instrumentu podlegającego transakcji
        valueLabel = QLabel()
        valueLabel.setText("Wartość")
        self.layout.addWidget(valueLabel, 9, 0)

        # Dodanie pola do wpisania wartości
        self.valueLineEdit              = QLineEdit()
        self.valueLineEdit.installEventFilter(self)
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
        self.taxComboBox.setCurrentText("Nie")
        self.layout.addWidget(self.taxComboBox, 10, 1)
        self.taxComboBox.currentTextChanged.connect(self.taxStateChosen)

        # Dodanie QLabel do opisu wartości podatku
        taxValueLabel = QLabel()
        taxValueLabel.setText("Wartość podatku")
        self.layout.addWidget(taxValueLabel, 11, 0)

        # Dodanie pola do wpisania wartości podatku
        self.taxValueLineEdit              = QLineEdit()
        self.taxValueLineEdit.installEventFilter(self)
        self.layout.addWidget(self.taxValueLineEdit, 11, 1)
        self.taxValueLineEdit.setEnabled(False)

        # Dodanie widgetu wyświetlającego informacje podczas wykonywania programu
        self.informationTextEdit = QTextEdit()
        self.layout.addWidget(self.informationTextEdit, 12, 0, 1, 3)
        self.informationTextEdit.setEnabled(False)

        # Dodanie przycisku do wysłania danych do BigQuery
        sendDataPushButton       = QPushButton()
        sendDataPushButton.setText("Wyślij dane do bazy")
        sendDataPushButton.pressed.connect(self.sendDataToBigQuery)
        sendDataPushButton.clicked.connect(self.close)
        self.layout.addWidget(sendDataPushButton, 13, 1)

        # Wyjście do poprzedniego okna
        returnButton             = QPushButton()
        returnButton.setText("Powrót")
        returnButton.pressed.connect(self.close)
        self.layout.addWidget(returnButton, 14, 1)

        self.layout.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignHCenter)
        self.layout.setSpacing(10)
        self.layout.setContentsMargins(150,20,150,20)
        self.layout.setColumnStretch(0, 1)
        self.layout.setColumnStretch(1, 2)
        self.layout.setColumnStretch(2, 1)
        self.setLayout(self.layout)

    # Metoda maskująca wprowadzane dane. Ograniczenie do danych liczbowych, backspace'a oraz pojedynczej kropki.
    def eventFilter(self, obj, event):
        if (obj is self.priceLineEdit or
            obj is self.commisionLineEdit or
            obj is self.valueLineEdit or
            obj is self.taxValueLineEdit) and event.type() == QEvent.Type.KeyPress:
            key = event.key()
            if obj is self.priceLineEdit:
                text = self.priceLineEdit.text()
            elif  obj is self.commisionLineEdit:
                text = self.commisionLineEdit.text()
            elif  obj is self.valueLineEdit:
                text = self.valueLineEdit.text()
            elif  obj is self.taxValueLineEdit:
                text = self.taxValueLineEdit.text()
            if key == Qt.Key.Key_Backspace:
                if '.' in text:
                    self.dot_entered = False  # Resetuj dot_entered, jeśli usunięto kropkę
                return False  # Pozwól na obsługę backspace
            if key == Qt.Key.Key_Period:
                if '.' in text:
                    return True  # Jeśli już jest kropka, zablokuj kolejną
                else:
                    self.dot_entered = True
                    return False  # Pozwól na wprowadzenie kropki
            if not event.text().isnumeric() and key != Qt.Key.Key_Period:  # Sprawdź, czy wprowadzony znak nie jest cyfrą
                return True
    
        elif obj is self.quantityLineEdit and event.type() == QEvent.Type.KeyPress:
            key = event.key()
            if key == Qt.Key.Key_Backspace:
                return False # Pozwól na obsługę Backspace
            if not event.text().isnumeric():
                return True # Blokuj inne znaki niż numeryczne
            
        return super().eventFilter(obj, event)

    
    # Metoda uruchamiająca się podczas zmiany typu instrumentu
    def instrumentTypeChanged(self):
        self.informationTextEdit.append("Zmieniono typ instrumentu.")
        self.instrumentComboBox.clear()
        self.instrumentComboBox.addItems(self.instrumentsDataFrame.query(f"Instrument_type == '{self.instrumentTypeComboBox.currentText()}'")['Ticker'].to_list())

    # Metoda sprawdza aktualny stan ComboBoxa i w zależności od niego definiuje widoczność lub nie pola 'self.taxValueLineEdit'
    def taxStateChosen(self, currentTextChanged):
        self.informationTextEdit.append("Zmieniono stan przycisku związanego z podatkiem. Obecny stan to: " 
                                        + currentTextChanged)
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

        # Sprawdzenie waluty i podpięcie ostatniego kursu waluty dla danego dnia. Wykorzystanie widoku Currency_view.
        if self.currentCurrency != 'PLN':
            self.lastDayCurrency = self.currenciesDataFrame.query(f"Currency== '{self.currentCurrency}' \
                                                                  & Currency_date== '{self.dateEditField}'") \
                                                                    ['last_currency_close'].\
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
        self.Transaction_price     = np.nan
        self.Transaction_amount    = np.nan
        self.Commision_id          = np.nan
        self.Dirty_bond_price      = 0.0
        self.Tax_value             = np.nan

        # Dodanie do obecnie ostatniego numeru transakcji wartości większej od 1
        self.Transaction_id        = self.maxTransactionId + 1
        self.Transaction_date      = self.dateDateEdit.date().toString("yyyy-MM-dd")
        self.Transaction_date      = pd.to_datetime(self.Transaction_date, format = "%Y-%m-%d")
        self.Transaction_type      = self.transactionsTypeComboBox.currentText()

        if self.Transaction_type   == "Sprzedaż":
            self.Transaction_type  = "Sell"
        elif self.Transaction_type == "Zakup":
            self.Transaction_type  = "Buy"
        else:
            self.Transaction_Type  = np.nan

    
        self.Currency              = self.currencyComboBox.currentText()
        if self.priceLineEdit.text():
            self.Transaction_price     = float(self.priceLineEdit.text())
        if self.quantityLineEdit.text():
            self.Transaction_amount    = float(self.quantityLineEdit.text())

        # Na podstawie Tickera wyznaczam Instrument_id
        self.Instrument_id         = self.instrumentsDataFrame.query(f"Ticker== '{self.instrumentComboBox.currentText()}'") \
                                                                                 ['Instrument_id'] .\
                                                                                 iloc[0]
        if self.commisionLineEdit.text():
            self.Commision_id          = float(self.commisionLineEdit.text())
        # self.Dirty_bond_price
        print(bool(self.taxComboBox.currentText()))
        self.Tax_paid                  = self.taxComboBox.currentText()
        if self.Tax_paid               == "Tak":
            self.Tax_paid              = True
        elif self.Tax_paid             == "Nie":
            self.Tax_paid              = False
        if self.taxValueLineEdit.text():
            self.Tax_value             = float(self.taxValueLineEdit.text())

        #if self.valueLineEdit.text():
        #    if self.Transaction_type == "Dywidenda":
        #        self.Transaction_price     = self.valueLineEdit.text()/ \
        #        (self.transactionsDataFrame.query(f"Ticker== '{self.instrumentComboBox.currentText()}'")['Amount'].iloc[0])/ \
        #        (self.currentCurrency)

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
        
        columns                = ["Transaction_id",
                                  "Transaction_date",
                                  "Transaction_type",
                                  "Currency",
                                  "Transaction_price",
                                  "Transaction_amount",
                                  "Instrument_id",
                                  "Commision_id",
                                  "Dirty_bond_price",
                                  "Tax_paid",
                                  "Tax_value"
                                  ]
        
        
        transaction_parameters_dataFrame = pd.DataFrame(data=[transaction_parameters],
                                                        columns = columns)
        
        return transaction_parameters_dataFrame
        
    # Metoda odpowiedzialna za wysłanie danych do BigQuery
    def sendDataToBigQuery(self):

        # przypisuję do zmiennej wynik pracy metody przygotowującej dane do eksportu
        transaction_data_to_export = self.PrepareDataForBigQueryExport()
        self.destination           = "Dane transakcyjne"


        # Tworzę obiekt BigQueryReaderAndExporter do eksportu danych do BQ i przekazuję mi dane projektu z klasy BigQueryProject
        bigQueryExporterObject = BigQueryReaderAndExporter(self.project, 
                                                           self.bqrae)
        self.message = bigQueryExporterObject.sendDataToBigQuery(transaction_data_to_export, self.destination)
        self.informationTextEdit.append(self.message)


# Klasa tymczasowo nieaktywna - do wprowadzenia w przyszłości
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
        

class MainWindow(QMainWindow,):
    def __init__(self, bigQueryProjectObject):
        
        super().__init__()
        self.title = "Portfel_inwestycyjny_Desktop_App"

        # Ustawienie parametrów okna i ustawienie widgetów
        self.setWindowTitle(self.title)
        self.setFixedSize(QSize(800,600))

        # Pobranie danych z obiektu do zmiennej
        self.bigQueryProjectObject = bigQueryProjectObject

        # Wywołanie na obiekcie klasy BigQueryReaderAndExporter metody 'downloadDataFromBigQuery' - pobranie 
        # danych z BigQuery, a następnie przypisanie wyniku pracy metody do zmiennych
        # Tą część będzie można ulepszyć - w tej chwili pobieranie danych powoduje zwiększenie czasu uruchamiania programu.
        # Warto będzie dodać okno wstępne z informacją o konieczności pobrania danych i oczekiwania.

        #  self.currenciesDataFrame, self.instrumentsDataFrame, self.transactionsDataFrame = \
        #  self.bqrae.downloadDataFromBigQuery()
        self.addWidgets()
    
    def addWidgets(self):
        layout = QGridLayout()

        # Dodanie etykiety pola odpowiedzialnego za pobranie nazwy projektu
        self.projectLabel = QLabel()
        self.projectLabel.setText("Nazwa projektu: ")
        layout.addWidget(self.projectLabel, 0, 0)

        # Dodanie pola odpowiedzialnego za pobranie nazwy projektu
        self.projectLineEdit            = QLineEdit()
        layout.addWidget(self.projectLineEdit, 0, 1)

        # Dodanie przycisku do zatwierdzenia nazwy projektu
        self.projectPushButton          = QPushButton("Zatwierdź")
        self.projectPushButton.clicked.connect(self.changeButtonState)
        layout.addWidget(self.projectPushButton, 0, 2)

        # Dodanie przycisku odpowiedzialnego za autoryzację
        self.authorizeButton = QPushButton("Autoryzuj")
        #self.authorizeButton.clicked.connect(self.addTransactions)
        layout.addWidget(self.authorizeButton, 1, 0, 1, 3)

        # Dodanie przycisku odpowiedzialnego za dodanie transakcji
        self.addTransaction = QPushButton("Dodaj transakcję")
        self.addTransaction.setProperty("id", "addTransaction")
        self.addTransaction.clicked.connect(lambda: self.addTransactionOrInstrument(self.addTransaction.property("id")))
        layout.addWidget(self.addTransaction, 2, 0, 1, 3)

        # Dodanie przycisku odpowiedzialnego za dodanie nowego instrumentu do słownika
        self.addInstr = QPushButton("Dodaj nowy instrument do słownika")
        self.addInstr.setProperty("id", "addInstr")
        self.addInstr.clicked.connect(lambda: self.addTransactionOrInstrument(self.addTransaction.property("id")))
        layout.addWidget(self.addInstr, 3, 0, 1, 3)

        # Dodanie przycisku odpowiedzialnego za zamknięcie okna głównego
        self.closeWindow = QPushButton("Zamknij")
        self.closeWindow.clicked.connect(self.close)
        layout.addWidget(self.closeWindow, 4, 0, 1, 3)

        layout.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignHCenter)
        layout.setSpacing(10)
        layout.setContentsMargins(200,20,200,20)

        # Zdefiniowanie głównego widgetu
        centralWidget = QWidget()
        centralWidget.setLayout(layout)
        self.setCentralWidget(centralWidget)

    ##########################################################################################
    # Obsługa wątku pobocznego

    # Gdy pobrano dane w tle przypisz je do zmiennych
    def on_data_ready(self, currenciesDataFrame, instrumentsDataFrame, transactionsDataFrame):
        self.currenciesDataFrame = currenciesDataFrame
        self.instrumentsDataFrame = instrumentsDataFrame
        self.transactionsDataFrame = transactionsDataFrame
    
    # Wykonaj działania w tle, gdy pole do wpisania nazwy projektu jest nieaktywne
    def performBackgroundOperations(self):
        if self.checkProjectLineEdit():
            # Tworzenie wątku, który wykona operacje w tle
            self.worker = WorkerThread(self.bqrae)
            self.worker.data_ready.connect(self.on_data_ready)
            self.worker.finished.connect(self.on_worker_finished)  # Po zakończeniu operacji wywołaj metodę
            self.worker.start()  # Rozpoczęcie pracy w tle

    # Wykonaj po zakończeniu działania w tle
    def on_worker_finished(self):
        self.currenciesDataFrame, self.instrumentsDataFrame, self.transactionsDataFrame = self.bqrae.downloadDataFromBigQuery()

        # Włącz przyciski
        self.addTransaction.setEnabled(True)
        self.addInstr.setEnabled(True)

        # Operacja zakończona
        print("Operacja zakończona!")

    ##############################################################################
    # Metoda sprawdzająca czy pole do wpisania danych projektu jest zatwierdzone
    def checkProjectLineEdit(self):
        if self.projectLineEdit.isEnabled():
            print("Musisz zatwierdzić projekt, zanim przejdziesz dalej")
            return 0
        else:
            return 1
    
    # Metoda tworząca obiekt klasy BigQueryProjectObject
    def createBigQueryProjectObject(self):
        self.bqrae = BigQueryReaderAndExporter(
            self.projectLineEdit.text(), 
            self.bigQueryProjectObject)
    
    # Gdy zatwierdzono projekt
    def changeButtonState(self):
        if self.projectLineEdit.isEnabled():
            self.projectLineEdit.setDisabled(True)  # Zablokowanie przycisku
            self.createBigQueryProjectObject()
            self.performBackgroundOperations()

            # Po włączeniu odświeżania w tle wyłącz wszystkie przyciski
            self.addTransaction.setDisabled(True)
            self.addInstr.setDisabled(True)

        else:
            self.projectLineEdit.setEnabled(True)  # Odblokowanie przycisku

    # Zdefiniowanie metody uruchamianej po naciśnięciu przycisku związanego z dodaniem transakcji lub instrumentu
    def addTransactionOrInstrument(self, id):
        # Jeśli wybór projektu został zatwierdzony
        if self.checkProjectLineEdit():
            try:
                if id == "addTransaction":
                    self.maxTransactionId = self.bqrae.downloadLastTransactionId()
                    self.dodajTransakcje = DodajTransakcje(self.projectLineEdit.text(),
                                                           self.bqrae,
                                                           self.currenciesDataFrame,
                                                           self.instrumentsDataFrame,
                                                           self.transactionsDataFrame,
                                                           self.maxTransactionId)
                    self.dodajTransakcje.show()
                elif id == "addInstr":
                    self.addInstrument = DodajInstrumentDoSlownika(self.instrumentsDataFrame)
                    self.addInstrument.show()
            except:
                print("Projekt nie istnieje. Proszę wybrać inny!")


# Deklaracja nazw datasetów i tabel
location               = 'europe-central2'

# Definicje nazw DataSetów
dataSetDaneIntrumentow = 'Dane_instrumentow'
dataSetCurrencies      = 'Waluty'
dataSetTransactions    = 'Transactions'
dataSetInflation       = 'Inflation'

# Definicje nazw tabel
tableDaily             = 'Daily'
tableInstrumentTypes   = 'Instrument_types'
tableInstruments       = 'Instruments'
tableTreasuryBonds     = 'Treasury_Bonds'
tableInflation         = 'Inflation'
tableTransactions      = 'Transactions'
tableCurrency          = 'Currency'

# Definicje nazw widoków
viewTransactionsView   = 'Transactions_view'
viewCurrencies         = 'Currency_view'
        
# Main part of the app
app = QApplication([]) 

# Inicjalizacje nieaktywne - do aktywacji podczas uruchamiania okna początkowego.
# window = InitialWindow()
# window.show()

bigQueryProjectObject = BigQueryProject(
    location,
    dataSetDaneIntrumentow,
    dataSetCurrencies,
    dataSetTransactions,
    dataSetInflation,
    tableDaily,
    tableInstrumentTypes,
    tableInstruments,
    tableTreasuryBonds,
    tableInflation,
    tableTransactions,
    tableCurrency,
    viewTransactionsView,
    viewCurrencies)
window = MainWindow(bigQueryProjectObject)
window.show()

app.exec()

