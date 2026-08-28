## Wdrożenie Cloud Schedulera, opartego o analogiczny job

### Wywołanie HTTP
```
gcloud scheduler jobs create http job-scheduler-name \
  --location=europe-central2 \
  --schedule="0 8 * * *" \
  --time-zone="Europe/Warsaw" \
  --uri="{URL}/{ENDPOINT}" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{"action":"run_task"}' \
  --oidc-service-account-email="{SA}" \
  --oidc-token-audience="{URL}"
```
### Wywołanie Pub/Sub
```
gcloud scheduler jobs create pubsub my_cron_job \
  --schedule="0 8 * * *" \
  --time-zone="Europe/Warsaw" \
  --topic="topic_name" \
  --message-body="{\"action\": \"run_task\"}" \
  --description="Task name" \
  --location=europe-central2
```