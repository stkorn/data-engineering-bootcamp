from airflow import DAG  # noqa: F401 - keeps Airflow's DAG-file safe-mode scanner from skipping this file
from greenery_pipeline_factory import create_greenery_pipeline


HEADER = [
    "address_id",
    "address",
    "zipcode",
    "state",
    "country",
]


def _map_address_row(record: dict) -> list:
    return [
        record["address_id"],
        record["address"],
        record["zipcode"],
        record["state"],
        record["country"],
    ]


dag = create_greenery_pipeline(
    data="addresses",
    header=HEADER,
    row_mapper=_map_address_row,
)
