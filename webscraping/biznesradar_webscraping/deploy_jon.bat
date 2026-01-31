@echo off
echo --- 1. Budowanie obrazu w Google Cloud ---
gcloud builds submit --tag gcr.io/projekt-inwestycyjny/biznesradar-webscraping .

echo --- 2. Aktualizacja konfiguracji z job.yaml ---
gcloud run jobs replace job.yaml

echo --- 3. Uruchomienie testowe zadania ---
gcloud run jobs execute biznesradar-webscraping --region europe-central2

echo --- GOTOWE ---
pause