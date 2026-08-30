from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import timezone

with DAG(
    dag_id="every_month_dag",
    description="A DAG that runs every month",
    schedule="@monthly",
    start_date=timezone.datetime(2026, 8, 30),
    catchup=False,
):
    echo_hello = BashOperator(
        task_id="echo_hello",
        bash_command="echo 'Hello every month!'",
    )
