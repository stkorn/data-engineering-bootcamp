from airflow import DAG  # noqa: F401 - keeps Airflow's DAG-file safe-mode scanner from skipping this file
from google.cloud import bigquery
from greenery_pipeline_factory import create_greenery_pipeline


# product_id,name,price,inventory
HEADER = [
    "product_id",
    "name",
    "price",
    "inventory",
]


def _map_product_row(record: dict) -> list:
    return [
        record["product_id"],
        record["name"],
        record["price"],
        record["inventory"],
    ]

SCHEMA = [
    bigquery.SchemaField("product_id", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("name", bigquery.SqlTypeNames.STRING),
    bigquery.SchemaField("price", bigquery.SqlTypeNames.FLOAT),
    bigquery.SchemaField("inventory", bigquery.SqlTypeNames.INTEGER),
]


dag = create_greenery_pipeline(
    data="products",
    header=HEADER,
    schema=SCHEMA,
    row_mapper=_map_product_row
)
