from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor


TABLES = [
    "gold_realestate_district_avg",
    "gold_realestate_top10",
    "gold_realestate_size_dist",
    "gold_realestate_age_avg",
    "gold_realestate_mom_change",
]


def verify_gold(**context):
    yyyymm = context["data_interval_start"].in_timezone(
        "Asia/Seoul"
    ).strftime("%Y%m")

    hook = PostgresHook(postgres_conn_id="gold_postgres")

    for table in TABLES:
        # 테이블 이름은 위의 고정 목록만 사용
        result = hook.get_first(
            f"SELECT COUNT(*) FROM {table} WHERE yyyymm = %s",
            parameters=(yyyymm,),
        )
        count = result[0]

        print(f"검증: {table}, 대상월={yyyymm}, 행 수={count}")

        if count <= 0:
            raise AirflowException(
                f"{table}에 {yyyymm} 집계 결과가 없습니다."
            )

    print("Gold 5개 테이블 검증 성공")


with DAG(
    dag_id="gold_realestate_aggregate",
    start_date=pendulum.datetime(2024, 12, 1, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2024, 12, 31, tz="Asia/Seoul"),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["homework", "Q3", "gold"],
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="silver_realestate_transform",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        mode="reschedule",
        poke_interval=30,
        timeout=1800,
    )

    run_gold = BashOperator(
        task_id="run_gold_spark",
        bash_command="""
        spark-submit \
          --master 'local[2]' \
          --driver-memory 1g \
          /opt/airflow/scripts/Q3/gold_spark_sql.py \
          --yyyymm '{{ data_interval_start.in_timezone("Asia/Seoul").strftime("%Y%m") }}'
        """,
        execution_timeout=timedelta(minutes=30),
    )

    verify = PythonOperator(
        task_id="verify_gold_tables",
        python_callable=verify_gold,
    )

    wait_for_silver >> run_gold >> verify