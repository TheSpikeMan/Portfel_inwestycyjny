## Komendy wdrożeniowe

### Tworzenie obrazu
```
docker build -t europe-central2-docker.pkg.dev/projekt-inwestycyjny/cloud-run-source-deploy/invest-project-cservice-eu-central2-webscraping-daily:latest .
```
### Wypychanie obrazu do Artifact Registry
```
docker push europe-central2-docker.pkg.dev/projekt-inwestycyjny/cloud-run-source-deploy/invest-project-cservice-eu-central2-webscraping-daily:latest
```
### Budowanie Cloud Run Service na bazie obrazu
```
gcloud run services replace job.yaml --region=europe-central2
```