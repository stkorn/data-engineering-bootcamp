from airflow import DAG  # type: ignore[import-not-found]  # noqa: F401 - keeps Airflow's DAG-file safe-mode scanner from skipping this file
from airflow.utils import timezone
from google.cloud import bigquery

from greenery_pipeline_factory import create_greenery_pipeline


HEADER = [
    "event_id",
    "session_id",
    "page_url",
    "created_at",
    "event_type",
    "user",
    "order",
    "product",
]

SCHEMA = [
    bigquery.SchemaField("event_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("session_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("page_url", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("event_type", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
]


def _map_event_row(record: dict) -> list:
    return [
        record["event_id"],
        record["session_id"],
        record["page_url"],
        record["created_at"],
        record["event_type"],
        record["user"],
        record["order"],
        record["product"],
    ]


dag = create_greenery_pipeline(
    data="events",
    header=HEADER,
    row_mapper=_map_event_row,
    schema=SCHEMA,
    is_partition=True,
    start_date=timezone.datetime(2021, 2, 1),
)
