import json
import logging
import os

from config import EXTERNAL_PROJECT_ID, TOPIC_NAME
from gobits import Gobits
from google.cloud import bigquery, pubsub_v1


def handler(request):
    """
    When triggered, this function fetches exectues a cost query in BigQuery.
    Results from this query are published on Pub/Sub topic.
    """

    dataset_id = os.getenv("DATASET_ID")

    with open("query.sql") as f:
        q = f.read()

    result = query(q, dataset_id, TOPIC_NAME)

    for item in result:
        logging.info(item)

    if result:
        metadata = Gobits.from_request(request=request).to_json()
        publish(result, metadata, TOPIC_NAME)


def query(q: str, dataset_id: str, topic_id: str):
    """
    Runs a legacy SQL query in BigQuery.
    """

    q = (
        q.replace("$DATASET", dataset_id)
        .replace(
            "$WHERE_CONDITION",
            ("DATE(usage_start_time) >= current_date() - 8" if topic_id else "TRUE"),
        )
        .replace(
            "$CASE_ONE",
            (
                "DATE(usage_start_time) = current_date() - 1"
                if topic_id
                else f"project.number = {EXTERNAL_PROJECT_ID}"
            ),
        )
        .replace(
            "$CASE_TWO",
            (
                "DATE(usage_start_time) BETWEEN (current_date() - 8) AND (current_date() - 2)"
                if topic_id
                else f"project.number != {EXTERNAL_PROJECT_ID}"
            ),
        )
    )

    client = bigquery.Client()

    results = client.query(q)

    return [dict(row) for row in results]


def publish(messages: list, metadata: dict, topic_id: str):
    """
    Publishes a json message to a Pub/Sub.
    """

    publisher = pubsub_v1.PublisherClient()

    for message in messages:

        prep_message = {"gobits": [metadata], "data": message}

        logging.info(f"Message ready for publishing: {prep_message}")

        if topic_id:
            future = publisher.publish(
                topic_id, json.dumps(prep_message).encode("utf-8")
            )
            logging.info(f"Published message with id {future.result()}")
