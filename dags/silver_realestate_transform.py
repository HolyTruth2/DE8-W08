from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor


with DAG(
    dag_id="silver_realestate_transform",
    start_date=pendulum.datetime(2024, 11, 1, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2024, 12, 31, tz="Asia/Seoul"),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["homework", "Q2", "silver"],
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        mode="reschedule",
        poke_interval=30,
        timeout=1800,
    )

    run_silver = BashOperator(
        task_id="run_silver_spark",
        bash_command="""
        spark-submit \
          --master 'local[2]' \
          --driver-memory 1g \
          --conf spark.ui.bindAddress=0.0.0.0 \
          /opt/airflow/scripts/Q2/silver_spark.py \
          --yyyymm '{{ data_interval_start.in_timezone("Asia/Seoul").strftime("%Y%m") }}' \
          --ui-wait-seconds 300
        """,
        execution_timeout=timedelta(minutes=30),
    )

    wait_for_bronze >> run_silver