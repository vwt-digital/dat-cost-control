import os
import json
import logging

from gobits import Gobits
from google.cloud import bigquery
from google.cloud import pubsub_v1

from config import TOPIC_NAME


def handler(request):
    """
    When triggered, this function fetches exectues a cost query in BigQuery.
    Results from this query are published on Pub/Sub topic.
    """

    dataset_id = os.getenv("DATASET_ID")

    with open("query.sql") as f:
        q = f.read()

    result = query(q, dataset_id)

    for item in result:
        logging.info(item)

    if result:
        metadata = Gobits.from_request(request=request).to_json()
        publish(result, metadata, TOPIC_NAME)


def query(q: str, dataset_id: str):
    """
    Runs a legacy SQL query in BigQuery.
    """

    q = q.replace("$DATASET", dataset_id)

    client = bigquery.Client()

    results = client.query(q)

    return [dict(row) for row in results]


def publish(messages: list, metadata: dict, topic_id: str):
    """
    Publishes a json message to a Pub/Sub.
    """

    publisher = pubsub_v1.PublisherClient()

    for message in messages:

        prep_message = {
            'gobits': [metadata],
            'data': message
        }

        future = publisher.publish(
            topic_id, json.dumps(prep_message).encode('utf-8')
        )

        logging.info(f"Published message with id {future.result()}")
