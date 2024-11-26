Projekt inwestycyjny jest autorskim pomysłem, którego głównym celem jest prowadzenie analizy instrumentów finansowych pod względem kryteriów umożliwiających skuteczne inwestowanie. 
Głównym celem projektu jest zbudowanie takich mechanizmów, dzięki którym możliwe będzie dokonywanie szybkich i trafnych decyzji inwestycyjnych na podstawie analizy fundamentalnej oraz analizy technicznej.

W tej chwili w projekcie funkcjonują cztery moduły przetwarzające dane giełdowe:

- webscraping_in_total - Moduł ściągania danych kursu, wolumenu oraz obrotu dla danego instrumentu finansowego oraz scrapujący dane kursów NBP dla walut USD oraz EUR.
- inflation_webscraping - Moduł scrapujący dane inflacyjne.
- google_finance_webscraping - Moduł scrapujący dane giełdowe dla danego instrumentu finansowego
- currencies_webscraping - Moduł pobierający dane walutowe dla głównej pary walutowej (EUR & USD) i zapisujące dane do tabeli w BigQuery.

Dwa pierwsze moduły napisane są w języku Python i przetwarzane przez mechanizmy chmurowe Google każdego wieczoru. Zasada działania mechanizmów scrapujących dane jest następująca:
- Inicjatorem scrapingu jest procedura stworzona w narzędziu Cloud Scheduler.
- O określonej godzinie, każdego dnia roboczego, procedura wysyła wiadomość w określonym temacie do wszystkich swoich subskrybentów w ramach usługi Pub/Sub.
- Subskrybentami usługi Pub/Sub są funkcje w usłudze Cloud Functions.
- Oba wyżej wymienione moduły przetwarzane są przez Cloud Functions.
- W ramach funkcji chmurowych, dokonywane jest pobranie danych oraz aktualizacja danych w określonych tabelach.

Pozostałe dwa moduły przeznaczone są do ad-hocowego scrapingu, przy czym ten ostatni jest również częścią 'webscraping_in_total'.

Tabele niezbędne do działania funkcji to:
- Tabela o nazwie Daily, przechowująca dane giełdowe,
- Tabela o nazwie Currency, przechowująca dane walutowe,
- Tabela o nazwie Inflation, przechowująca dane polskiej inflacji,
- Tabela o nazwie Dane_instrumentow, przechowująca dane instrumentów,
- Tabela o nazwie Instrument_types, przechowująca dane typów instrumentów,
- Tabela o nazwie Transactions, przechowująca dane transakcyjne,
- Tabela o nazwie Treasury_Bonds, przechowująca dane oprocentowań obligacji skarbowych,
- Widok o nazwie Transaction_view, przechowujący wszystkie dane transakcyjne zestawione z danymi z innych tabel.

Nazwy tabel można z łatwością dostosować do własnych potrzeb, w obu kodach.

Oprócz mechanizmu scrapingu, zaimplementowano aplikację desktową o nazwie 'Portfel_inwestycyjny_DesktopApp.py'.
Docelowym celem aplikacji jest:
- Aktualizacja danych zakupowo-sprzedażowych na bazie, z poziomu interfejsu (wdrożono)
- Wizualizacje i analizy techniczne (do wprowadzenia)
- Analiza fundamentalna na podstawie scrapowanych danych giełdowych (do wprowadzenia)


