## Wdrożenie Cloud Schedulera, opartego o analogiczny job

### Wywołanie HTTP
```
gcloud scheduler jobs create http my-cron-job \
  --schedule="0 8 * * *" \
  --uri="${SERVICE_URL}/api/tasks" \
  --http-method=POST \
  --location=europe-central2 \
  --oidc-service-account-email="moj-scheduler-sa@${PROJECT_ID}.iam.gserviceaccount.com"
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