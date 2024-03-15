from PyQt6.QtWidgets import QApplication, QMainWindow, QPushButton, QGridLayout, QWidget, QVBoxLayout, QLabel, QComboBox, QLineEdit
from PyQt6.QtCore import QSize, Qt

class DodajInstrumentDoSlownika(QWidget):

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
        layout.addWidget(sendDataPushButton, 4, 0)

        # Wyjście do poprzedniego okna
        returnButton             = QPushButton()
        returnButton.setText("Powrót")
        returnButton.pressed.connect(self.close)
        layout.addWidget(returnButton, 5, 0)

        self.setLayout(layout)
    
    # Metoda odpowiedzialna za wysłanie danych do BigQuery
    def sendDataToBigQuery(self):
        print("Wysyłam dane do BigQuery.")  


class DodajTransakcje(QWidget):

    def __init__(self):
        super().__init__()
        print("Dodaję transakcję")

        # Ustawienie okna oraz załadowanie widgetów
        self.setWindowTitle("Transakcje")
        self.setFixedSize(QSize(800,500))
        self.addWidgets()
    
    def addWidgets(self):
        layout = QGridLayout()

        # Dodanie ComboBoxa do wyboru typu danych
        transactionsTypeComboBox = QComboBox()
        transactionsTypeComboBox.addItems(["Sprzedaż", "Zakup", "Wykup", "Dywidenda", "Odsetki"])
        layout.addWidget(transactionsTypeComboBox, 0, 0)

        # Dodanie przycisku do wysłania danych do BigQuery
        sendDataPushButton       = QPushButton()
        sendDataPushButton.setText("Wyślij dane do bazy")
        sendDataPushButton.pressed.connect(self.sendDataToBigQuery)
        layout.addWidget(sendDataPushButton, 5, 0)

        # Dodanie pola do wpisania ilości zakupionego waloru
        quantityLineEdit         = QLineEdit()
        layout.addWidget(quantityLineEdit, 3, 0)

        # Dodanie pola do ceny zakupionego waloru
        priceLineEdit            = QLineEdit()
        layout.addWidget(priceLineEdit, 4, 0)

        # Wyjście do poprzedniego okna
        returnButton             = QPushButton()
        returnButton.setText("Powrót")
        returnButton.pressed.connect(self.close)
        layout.addWidget(returnButton, 6, 0)

        self.setLayout(layout)
    
    # Metoda odpowiedzialna za wysłanie danych do BigQuery
    def sendDataToBigQuery(self):
        print("Wysyłam dane do BigQuery.")

class MainWindow(QMainWindow):
    def __init__(self, title):
        super().__init__()

        # Ustawienie parametrów okna i ustawienie widgetów
        self.setWindowTitle(title)
        self.setFixedSize(QSize(800,500))
        self.addWidgets()
    
    def addWidgets(self):
        layout = QGridLayout()

        # Dodanie przycisku odpowiedzialnego za dodanie transakcji
        button1 = QPushButton("Dodaj transakcję")
        button1.clicked.connect(self.addTransactions)
        layout.addWidget(button1, 0, 0)

        # Dodanie przycisku odpowiedzialnego za dodanie nowego instrumentu do słownika
        button2 = QPushButton("Dodaj nowy instrument do słownika")
        button2.clicked.connect(self.addInstrument)
        layout.addWidget(button2, 1, 0)

        # Dodanie przycisku odpowiedzialnego za zamknięcie okna głównego
        button3 = QPushButton("Zamknij")
        button3.clicked.connect(self.close)
        layout.addWidget(button3, 2, 0)

        centralWidget = QWidget()
        centralWidget.setLayout(layout)
        self.setCentralWidget(centralWidget)
    
    # Zdefiniowanie metody uruchamianej po naciśnięciu przycisku 'button1'
    def addTransactions(self):
        self.dodajTransakcje = DodajTransakcje()
        self.dodajTransakcje.show()
    
    # Zdefiniowane metody uruchamianej po naciśnięciu przycisku 'button2'
    def addInstrument(self):
        self.addInstrument = DodajInstrumentDoSlownika()
        self.addInstrument.show()

# Main part of the app
app = QApplication([])
title = "Portfel_inwestycyjny_Desktop_App"     
window = MainWindow(title)
window.show()

app.exec()

