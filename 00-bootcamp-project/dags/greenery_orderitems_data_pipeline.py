from airflow import DAG  # type: ignore[import-not-found]  # noqa: F401 - keeps Airflow's DAG-file safe-mode scanner from skipping this file
from airflow.utils import timezone
from google.cloud import bigquery

from greenery_pipeline_factory import create_greenery_pipeline

# {
#         "order": "5e75b8f4-e03e-462f-8a91-027bfaf3e8b4",
#         "quantity": 3,
#         "product": "c7050c3b-a898-424d-8d98-ab0aaad7bef4"
    # }
HEADER = [
    "order",
    "quantity",
    "product",
]

SCHEMA = [
    bigquery.SchemaField("order", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("quantity", bigquery.SqlTypeNames.INTEGER),
    bigquery.SchemaField("product", bigquery.SqlTypeNames.STRING),
]


def _map_event_row(record: dict) -> list:
    return [
        record["order"],
        record["quantity"],
        record["product"],
    ]


dag = create_greenery_pipeline(
    data="order_items",
    header=HEADER,
    row_mapper=_map_event_row,
    schema=SCHEMA,
)
