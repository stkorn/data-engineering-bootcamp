from airflow import DAG  # type: ignore[import-not-found]  # noqa: F401 - keeps Airflow's DAG-file safe-mode scanner from skipping this file
from airflow.utils import timezone
from google.cloud import bigquery

from greenery_pipeline_factory import create_greenery_pipeline


HEADER = [
    "order_id",
    "created_at",
    "order_cost",
    "shipping_cost",
    "order_total",
    "tracking_id",
    "shipping_service",
    "estimated_delivery_at",
    "delivered_at",
    "status",
    "user_id",
    "promo_id",
    "address_id",
]

SCHEMA = [
    bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("order_cost", bigquery.SqlTypeNames.FLOAT),
    bigquery.SchemaField("shipping_cost", bigquery.SqlTypeNames.FLOAT),
    bigquery.SchemaField("order_total", bigquery.SqlTypeNames.FLOAT),
    bigquery.SchemaField("tracking_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("shipping_service", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("estimated_delivery_at", bigquery.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("delivered_at", bigquery.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("status", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("promo_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
]


def _map_event_row(record: dict) -> list:
    return [
        record["order_id"],
        record["created_at"],
        record["order_cost"],
        record["shipping_cost"],
        record["order_total"],
        record["tracking_id"],
        record["shipping_service"],
        record["estimated_delivery_at"],
        record["delivered_at"],
        record["status"],
        record["user"],
        record["promo"],
        record["address"],
    ]


dag = create_greenery_pipeline(
    data="orders",
    header=HEADER,
    row_mapper=_map_event_row,
    schema=SCHEMA,
    is_partition=True,
    start_date=timezone.datetime(2021, 2, 1),
)
