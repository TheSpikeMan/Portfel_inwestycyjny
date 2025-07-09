---------- Wersja PL ----------
Projekt inwestycyjny jest autorskim pomysłem, którego głównym celem jest prowadzenie analizy instrumentów finansowych pod kątem kryteriów umożliwiających skuteczne inwestowanie.
Głównym celem projektu jest zbudowanie mechanizmów umożliwiających podejmowanie szybkich i trafnych decyzji inwestycyjnych na podstawie analizy fundamentalnej oraz analizy technicznej.

Obecnie w projekcie funkcjonują cztery moduły przetwarzające dane giełdowe:
- webscraping_in_total – moduł ściągający dane kursu, wolumenu oraz obrotu dla danego instrumentu finansowego oraz scrapujący dane kursów NBP dla walut USD oraz EUR.
- inflation_webscraping – moduł scrapujący dane inflacyjne.
- google_finance_webscraping – moduł scrapujący dane giełdowe dla danego instrumentu finansowego.
- currencies_webscraping – moduł pobierający dane walutowe dla głównej pary walutowej (EUR & USD) i zapisujący dane do tabeli w BigQuery.

Dwa pierwsze moduły są napisane w języku Python i przetwarzane przez mechanizmy chmurowe Google każdego wieczoru. Zasada działania mechanizmów scrapujących dane jest następująca:
- Inicjatorem scrapingu jest procedura stworzona w narzędziu Cloud Scheduler.
- O określonej godzinie, każdego dnia roboczego, procedura wysyła wiadomość w określonym temacie do wszystkich swoich subskrybentów w ramach usługi Pub/Sub.
- Subskrybentami usługi Pub/Sub są funkcje w usłudze Cloud Functions.
- Oba wyżej wymienione moduły są przetwarzane przez Cloud Functions.
- W ramach funkcji chmurowych wykonywane jest pobranie danych oraz ich aktualizacja w określonych tabelach.

Pozostałe dwa moduły przeznaczone są do ad-hocowego scrapingu, przy czym ostatni z nich jest również częścią modułu webscraping_in_total.

Tabele niezbędne do działania funkcji to:
- tabela o nazwie Daily, przechowująca dane giełdowe,
- tabela o nazwie Currency, przechowująca dane walutowe,
- tabela o nazwie Inflation, przechowująca dane polskiej inflacji,
- tabela o nazwie Dane_instrumentow, przechowująca dane instrumentów,
- tabela o nazwie Instrument_types, przechowująca dane typów instrumentów,
- tabela o nazwie Transactions, przechowująca dane transakcyjne,
- tabela o nazwie Treasury_Bonds, przechowująca dane oprocentowań obligacji skarbowych,
- widok o nazwie Transaction_view, zestawiający wszystkie dane transakcyjne z danymi z innych tabel.

Nazwy tabel można z łatwością dostosować do własnych potrzeb w obu kodach.

Oprócz mechanizmu scrapingu, zaimplementowano aplikację desktopową (z wykorzystaniem biblioteki PyQt6) o nazwie Portfel_inwestycyjny_DesktopApp.py.
Docelowym celem aplikacji jest:
- aktualizacja danych zakupowo-sprzedażowych w bazie z poziomu interfejsu (wdrożone),
- wizualizacje i analizy techniczne (do wprowadzenia),
- analiza fundamentalna na podstawie scrapowanych danych giełdowych (do wprowadzenia).

Efektem końcowym projektu jest wizualizacja w Microsoft Power BI, prezentująca dane giełdowe dla instrumentów wchodzących w skład portfela inwestycyjnego oraz 
same dane transakcyjne. Wszystko to jest przedstawione za pomocą kilku kart, prezentujących wybrane aspekty analizy portfela inwestycyjnego – 
zarówno ogólne wskaźniki powodzenia inwestycji, takie jak stopa zwrotu czy zysk transakcyjny, jak i bardziej szczegółowe analizy, 
np. zysk per instrument czy stopa dywidendy z posiadanych instrumentów.
Szczegóły dashboardu dostępne są na pokazowych danych w pliku „Dashboard inwestycyjny - portfel pokazowy.pdf” dostępnym w repozytorium.

Przyszłościowym celem projektu jest migracja do formy aplikacji webowej.

