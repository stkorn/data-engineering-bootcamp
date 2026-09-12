from airflow import DAG  # noqa: F401 - keeps Airflow's DAG-file safe-mode scanner from skipping this file
from google.cloud import bigquery
from greenery_pipeline_factory import create_greenery_pipeline


# promo_id,discount,status
HEADER = [
    "promo_id",
    "discount",
    "status"
]

def _map_promo_row(record: dict) -> list:
    return [
        record["promo_id"],
        record["discount"],
        record["status"],
    ]

SCHEMA = [
    bigquery.SchemaField("promo_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("discount", bigquery.SqlTypeNames.FLOAT),
    bigquery.SchemaField("status", bigquery.SqlTypeNames.STRING),
]


dag = create_greenery_pipeline(
    data="promos",
    header=HEADER,
    schema=SCHEMA,
    row_mapper=_map_promo_row
)
