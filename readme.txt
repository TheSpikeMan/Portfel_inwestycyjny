Projekt inwestycyjny jest autorskim pomysłem, którego głównym celem jest prowadzenie analizy instrumentów finansowych pod względem kryteriów umożliwiających skuteczne inwestowanie. Głównym celem projektu jest zbudowanie takich mechanizmów, dzięki którym możliwe będzie dokonywanie szybkich i trafnych decyzji inwestycyjnych na podstawie analizy fundamentalnej oraz analizy technicznej.

W tej chwili w projekcie funkcjonują trzy moduły przetwarzające dane giełdowe:

- webscraping_investing_data - Moduł ściągania danych kursu, wolumenu oraz obrotu dla danego instrumentu finansowego.
- currencies_webscraping - Moduł scrapujący dane kursów NBP dla walut USD oraz EUR.
- inflation_webscraping - Moduł scrapujący dane inflacyjne.

Wszystkie zaprezentowane moduły napisane są w języku Python i przetwarzane przez mechanizmy chmurowe Google każdego wieczoru. Zasada działania mechanizmów scrapujących dane jest następująca:
- Inicjatorem scrapingu jest procedura stworzona w narzędziu Cloud Scheduler.
- O określonej godzinie, każdego dnia roboczego, procedura wysyła wiadomość w określonym temacie do wszystkich swoich subskrybentów w ramach usługi Pub/Sub.
- Subskrybentami usługi Pub/Sub są funkcje w usłudze Cloud Functions.
- Oba wyżej wymienione moduły przetwarzane są przez Cloud Functions.
- W ramach funkcji chmurowych, dokonywane jest pobranie danych oraz aktualizacja danych w określonych tabelach.

Tabele niezbędne do działania funkcji to:
- Tabela o nazwie Daily, przechowująca dane giełdowe,
- Tabela o nazwie Currency, przechowująca dane walutowe,
- Tabela o nazwie Inflation, przechowująca dane polskiej inflacji.

Nazwy tabel można z łatwością dostosować do własnych potrzeb, w obu kodach.



