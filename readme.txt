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

---------- ENG version ----------
The investment project is an original idea whose main goal is to conduct analysis of financial instruments based on criteria enabling effective investing.
The primary objective of the project is to build mechanisms that allow making quick and accurate investment decisions based on fundamental and technical analysis.

Currently, the project includes four modules processing stock market data:
- webscraping_in_total – a module that collects price, volume, and turnover data for a given financial instrument, and also scrapes NBP exchange rates for USD and EUR currencies.
- inflation_webscraping – a module that scrapes inflation data.
- google_finance_webscraping – a module that scrapes stock market data for a given financial instrument.
- currencies_webscraping – a module that retrieves currency data for the main currency pair (EUR & USD) and saves the data into a table in BigQuery.

The first two modules are written in Python and are processed by Google Cloud mechanisms every evening. The operation of the scraping mechanisms is as follows:
- The scraping is initiated by a procedure created in Cloud Scheduler.
- At a specified time on each business day, the procedure sends a message on a specific topic to all its subscribers within the Pub/Sub service.
- The subscribers of the Pub/Sub service are functions in Cloud Functions.
- Both of the above modules are processed by Cloud Functions.
- Within these cloud functions, data is fetched and updated in designated tables.

The remaining two modules are intended for ad-hoc scraping, with the last one also being part of the webscraping_in_total module.

The tables required for the functions to operate are:
- Daily table, storing stock market data,
- Currency table, storing currency data,
- Inflation table, storing Polish inflation data,
- Dane_instrumentow table, storing instrument data,
- Instrument_types table, storing instrument type data,
- Transactions table, storing transactional data,
- Treasury_Bonds table, storing treasury bond interest rates,
- Transaction_view view, combining all transactional data with data from other tables.

Table names can be easily adapted to individual needs in both codebases.

In addition to the scraping mechanism, a desktop application (built using the PyQt6 library) named Portfel_inwestycyjny_DesktopApp.py has been implemented.
The intended goals of the application are:
- updating purchase and sale data in the database via the interface (implemented),
- visualizations and technical analyses (to be implemented),
- fundamental analysis based on scraped stock market data (to be implemented).

The final outcome of the project is a Microsoft Power BI visualization presenting stock market data for the instruments included in the investment portfolio as well as the transactional data itself.
All of this is presented via several pages that showcase selected aspects of portfolio analysis — both general indicators of investment success, such as return rate and transactional profit, and more detailed analyses,
e.g., profit per instrument or dividend yield from owned instruments.
Details of the dashboard are available using sample data in the file "Dashboard inwestycyjny - portfel pokazowy.pdf" located in the repository.

A future goal of the project is to migrate it into a web application form.

