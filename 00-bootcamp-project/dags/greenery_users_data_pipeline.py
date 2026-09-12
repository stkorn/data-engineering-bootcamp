from airflow import DAG  # type: ignore[import-not-found]  # noqa: F401 - keeps Airflow's DAG-file safe-mode scanner from skipping this file
from airflow.utils import timezone
from google.cloud import bigquery

from greenery_pipeline_factory import create_greenery_pipeline


# user_id,first_name,last_name,email,phone_number,created_at,updated_at,address
HEADER = [
    "user_id",
    "first_name",
    "last_name",
    "email",
    "phone_number",
    "created_at",
    "updated_at",
    "address"
]

SCHEMA = [
    bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("first_name", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("last_name", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("email", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("phone_number", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("updated_at", bigquery.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField("address", bigquery.SqlTypeNames.STRING),
]

def _map_event_row(record: dict) -> list:
    return [
        record["user_id"],
        record["first_name"],
        record["last_name"],
        record["email"],
        record["phone_number"],
        record["created_at"],
        record["updated_at"],
        record["address"],
    ]


dag = create_greenery_pipeline(
    data="users",
    header=HEADER,
    row_mapper=_map_event_row,
    schema=SCHEMA,
    is_partition=True,
    start_date=timezone.datetime(2020, 10, 1),
)
