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
from PyQt6.QtCore import QSize, Qt, QCalendar
from PyQt6.QtGui import QFont
from google.cloud import bigquery
import pandas as pd


class AbstractClass(QWidget):
    def __init__(self):
        super().__init__()
        self.project  = project
        self.location = location
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
        self.viewTransactionsView   = 'Transactions_view'
        self.tableCurrency          = 'Currency'

        self.downloadDataFromBigQuery()

    def downloadDataFromBigQuery(self):
        client = bigquery.Client(project    = self.project,
                                 location   = self.location)
        
        # Downloading Currencies Data from Big Query
        queryCurrencies = f"""
        SELECT
            Currency_date     AS Currency_Date,
            Currency          AS Currency,
            Currency_close    AS Currency_close
        FROM `{self.project}.{self.dataSetCurrencies}.{self.tableCurrency}`
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


class DodajInstrumentDoSlownika(AbstractClass):

    def __init__(self):
        super().__init__()
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
        print("Wysyłam dane do BigQuery.")  

class DodajTransakcje(AbstractClass):

    def __init__(self):
        super().__init__()
        print("Dodaję transakcję")

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
        #transactionsTypeComboBox.addItems(["Tutaj zapytanie do bazy BQ"])
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
        self.layout.addWidget(self.currencyComboBox, 6, 2)

        # Dodanie QLabel do opisu prowizji instrumentu podlegającego transakcji
        commisionLabel = QLabel()
        commisionLabel.setText("Prowizja")
        self.layout.addWidget(commisionLabel, 7, 0)

        # Dodanie pola do prowizji zakupionego waloru
        self.commisionLineEdit          = QLineEdit()
        self.layout.addWidget(self.commisionLineEdit, 7, 1)

        # Dodanie QLabel do opisu wartośći instrumentu podlegającego transakcji
        valueLabel = QLabel()
        valueLabel.setText("Wartość")
        self.layout.addWidget(valueLabel, 8, 0)

        # Dodanie pola do wpisania wartości
        self.valueLineEdit              = QLineEdit()
        self.layout.addWidget(self.valueLineEdit, 8, 1)

        # Dodanie przycisku do przeliczenia wartości
        self.valueCalculateButton       = QPushButton("Przelicz wartość")
        self.valueCalculateButton.pressed.connect(self.CalculateValue)
        self.layout.addWidget(self.valueCalculateButton)

        # Dodanie QLabel do opisu podatku
        taxLabel = QLabel()
        taxLabel.setText("Czy zapłacono podatek?")
        self.layout.addWidget(taxLabel, 9, 0)

        # Dodanie ComboBoxa do wyboru waluty instrumentu finansowego
        self.taxComboBox = QComboBox()
        self.taxComboBox.addItems(["Tak", "Nie"])
        self.taxComboBox.setCurrentText("Nie")
        self.layout.addWidget(self.taxComboBox, 9, 1)
        self.taxComboBox.currentTextChanged.connect(self.taxStateChosen)

        # Dodanie QLabel do opisu wartości podatku
        taxValueLabel = QLabel()
        taxValueLabel.setText("Wartość podatku")
        self.layout.addWidget(taxValueLabel, 10, 0)

        # Dodanie pola do wpisania wartości podatku
        self.taxValueLineEdit              = QLineEdit()
        self.layout.addWidget(self.taxValueLineEdit, 10, 1)

        # Dodanie przycisku do wysłania danych do BigQuery
        sendDataPushButton       = QPushButton()
        sendDataPushButton.setText("Wyślij dane do bazy")
        sendDataPushButton.pressed.connect(self.sendDataToBigQuery)
        self.layout.addWidget(sendDataPushButton, 11, 1)

        # Wyjście do poprzedniego okna
        returnButton             = QPushButton()
        returnButton.setText("Powrót")
        returnButton.pressed.connect(self.close)
        self.layout.addWidget(returnButton, 12, 1)

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

    # Przekazuję wybraną datę do pola tekstowego
    def DateChanged(self):
        #selected_date = self.calendarWidget.selectedDate().toString("yyyy-MM-dd")
        selected_date = self.calendarWidget.selectedDate()
        self.dateDateEdit.setDate(selected_date)
        self.calendarWidget.close()
        self.layout.setContentsMargins(150,20,150,20)

    # Metoda OpenCalendar tworzy obiekt QCalendarWidget i uruchamia metodę DateChanged, która pobiera datę z kalendarza
    # i ustawia ją w polu bok. Na końcu obiekt jest zamykany.
    def OpenCalendar(self):
        self.calendarWidget = QCalendarWidget()
        self.calendarWidget.selectionChanged.connect(self.DateChanged)
        self.layout.addWidget(self.calendarWidget, 1, 2, 7, 10)

    # Metoda oblicza wartość na podstawie ilości oraz ceny
    def CalculateValue(self):
        if self.priceLineEdit.text() != "" and self.quantityLineEdit.text() != "":
            value = str(
                        round(
                            float(self.priceLineEdit.text()) * float(self.quantityLineEdit.text()), 
                        2))
            self.valueLineEdit.setText(value)
        else:
            pass    

    def TransactionTypeBuyChosen(self, currentTextChanged):
        if currentTextChanged == "Zakup":
            self.taxComboBox.setEnabled(False)
            self.taxValueLineEdit.setEnabled(False)
        else:
            self.taxComboBox.setEnabled(True)
            self.taxValueLineEdit.setEnabled(True)

    # Metoda odpowiedzialna za wysłanie danych do BigQuery
    def sendDataToBigQuery(self):
        print("Wysyłam dane do BigQuery.")

class MainWindow(QMainWindow):
    def __init__(self, title, project, location):
        super().__init__()
        # Ustawienie parametrów okna i ustawienie widgetów
        self.setWindowTitle(title)
        self.setFixedSize(QSize(800,500))
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
        self.dodajTransakcje = DodajTransakcje()
        self.dodajTransakcje.show()
    
    # Zdefiniowane metody uruchamianej po naciśnięciu przycisku 'AddInstr'
    def addInstrument(self):
        self.addInstrument = DodajInstrumentDoSlownika()
        self.addInstrument.show()

# Main part of the app
app = QApplication([])
title = "Portfel_inwestycyjny_Desktop_App"
project = 'projekt-inwestycyjny'
location = 'europe-central2'   
window = MainWindow(title, project, location)
window.show()

app.exec()

