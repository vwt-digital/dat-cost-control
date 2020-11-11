<<<<<<< Updated upstream
This repository contains a Google Cloud function which can be readily deployed to the Google Cloud Platform. The purpose of this cloud function is to read data from a

a file with records from a storage bucket in different formats. It then checks whether there are new records in the file according to a state saved in Google Cloud Datastore or Google Cloud Storage. Finally it publishes new records according to a predefined schema and saves the those records in the state for the next run.

=======
This repository contains a Google Cloud function which can be readily deployed to the Google Cloud Platform. The purpose of this cloud function is to read data from a billing dataset in BigQuery and publish billing anomalies to Pub/Sub. An example query can be found in query.sql.
>>>>>>> Stashed changes

## Deployment

```
gcloud functions deploy cost-monitoring-function \
  --entry-point=handler \
  --runtime=python37 \
  --project=my-project \
  --region=europe-west1 \
  --memory=512MB \
  --timeout=120s \
  --set-env-vars PROJECT_ID=${PROJECT_ID} \
  --set-env-vars TOPIC_ID=${TOPIC_ID} \
  --set-env-vars DATASET_ID="project:dataset.gcp_billing_export_v1_XXXXXX_XXXXXX_XXXXXX"
```

## Testing

Install dependencies from `requirements.txt`, and run the following command:

```
python3 -c "from main import handler; handler(request=None)"
```
