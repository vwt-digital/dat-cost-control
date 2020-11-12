import os
import json
import logging

from google.cloud import bigquery
from google.cloud import pubsub_v1


def handler(request):
    """
    When triggered, this function fetches exectues a cost query in BigQuery.
    Results from this query are published on Pub/Sub topic.
    """

    project_id = os.getenv("PROJECT_ID")
    topic_id = os.getenv("TOPIC_ID")
    dataset_id = os.getenv("DATASET_ID")

    with open("query.sql") as f:
        q = f.read()

    result = query(q, dataset_id)

    for item in result:
        logging.info(item)

    publish(result, project_id, topic_id)


def query(q: str, dataset_id: str):
    """
    Runs a legacy SQL query in BigQuery.
    """

    q = q.replace("$DATASET", dataset_id)

    client = bigquery.Client()

    results = client.query(q)

    return [dict(row) for row in results]


def publish(messages: list, project_id: str, topic_id: str):
    """
    Publishes a json message to a Pub/Sub.
    """

    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(project_id, topic_id)

    for message in messages:

        data = json.dumps(message).encode('utf-8')

        future = publisher.publish(
            topic_path, data
        )

        logging.info(f"Published message with id {future.result()}")
