from PyQt6.QtWidgets import QApplication, QWidget

class Window(QWidget):
    def __init__(self, title):
        super().__init__()
    
    def setWindowParameters(self):
        self.setWindowTitle(title)

# Main part of the app
app = QApplication([])
title = "Portfel_inwestycyjny_Desktop_App"     
window = Window(title)
window.setWindowParameters()
window.show()

app.exec()