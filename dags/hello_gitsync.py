import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="hello_gitsync",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["Q4", "git-sync"],
) as dag:

    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Hello from git-sync! Student: 이성진'",
    )

    check_environment = BashOperator(
        task_id="check_environment",
        bash_command="date && hostname && echo 'git-sync test success'",
    )

    hello >> check_environment