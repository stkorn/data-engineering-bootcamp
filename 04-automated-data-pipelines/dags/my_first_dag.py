from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import timezone

with DAG(
    dag_id="my_first_dag",
    schedule=None,
    start_date=timezone.datetime(2026, 1, 1),
):
    hello_task = BashOperator(
        task_id="hello_task",
        bash_command="echo 'Hello World!'",
    )

    world_task = BashOperator(
        task_id="world",
        bash_command="echo 'World!'",
    )

    hello_task >> world_task
