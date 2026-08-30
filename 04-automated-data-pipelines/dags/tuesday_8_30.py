from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import timezone

with DAG(
    dag_id="8_30_tuesday_dag",
    description="A DAG that runs every Tuesday at 8:30 AM",
    schedule="30 8 * * 2",
    start_date=timezone.datetime(2026, 8, 30),
    catchup=False,
):
    echo_hello = BashOperator(
        task_id="echo_hello_tuesday",
        bash_command="echo 'Hello from tuesday morning'",
    )
