import os
from datetime import datetime, timedelta
from urllib.parse import unquote
import xml.etree.ElementTree as ET

import boto3
import pendulum
import requests

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup


BUCKET = "realestate-leesungjin"
REGION = "ap-northeast-2"
LAWD_CODES = ["11680", "11650", "11710", "11440", "11170", "11200"]
API_URL = (
    "https://apis.data.go.kr/1613000/"
    "RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
)


def collect_region(lawd_cd, **context):
    print(f"collector=이성진, time={datetime.now()}, lawd={lawd_cd}")

    yyyymm = context["data_interval_start"].in_timezone(
        "Asia/Seoul"
    ).strftime("%Y%m")

    api_key = os.getenv("APT_API_KEY", "").strip()
    if not api_key:
        raise AirflowException("APT_API_KEY가 비어 있습니다.")

    s3 = boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["AWS_SECRET_KEY"],
    )

    page = 1
    collected = 0

    while True:
        params = {
            "serviceKey": unquote(api_key),
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": yyyymm,
            "pageNo": page,
            "numOfRows": 1000,
        }

        try:
            response = requests.get(API_URL, params=params, timeout=60)
        except requests.RequestException:
            raise AirflowException("API 네트워크 요청 실패") from None

        if response.status_code != 200:
            raise AirflowException(
                f"API HTTP 오류: {response.status_code}"
            )

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError:
            print(f"XML 파싱 실패: lawd={lawd_cd}, page={page}")
            return {"status": "xml_error", "count": collected}

        result_code = root.findtext(".//resultCode")
        if result_code not in {"000", "00"}:
            raise AirflowException(
                f"API 응답 오류: resultCode={result_code}"
            )

        items = root.findall(".//item")
        total = int(root.findtext(".//totalCount", "0"))

        if page == 1:
            key = f"bronze/{yyyymm}/{lawd_cd}.xml"
        else:
            key = f"bronze/{yyyymm}/{lawd_cd}/page_{page}.xml"

        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=response.content,
            ContentType="application/xml",
        )
        print(f"저장 완료: s3://{BUCKET}/{key}, 건수={len(items)}")

        collected += len(items)

        if collected >= total:
            break

        if not items:
            raise AirflowException(
                "전체 건수보다 적게 수집했지만 다음 페이지가 비었습니다."
            )

        page += 1

    status = "empty" if collected == 0 else "ok"
    return {"status": status, "count": collected}


def choose_branch(**context):
    results = context["ti"].xcom_pull(
        task_ids=[
            f"collect_group.collect_{code}"
            for code in LAWD_CODES
        ]
    )

    if any(result["status"] == "xml_error" for result in results):
        return "handle_xml_error"

    if any(result["status"] == "empty" for result in results):
        return "handle_empty"

    return "collect_success"


def handle_empty(**context):
    results = context["ti"].xcom_pull(
        task_ids=[
            f"collect_group.collect_{code}"
            for code in LAWD_CODES
        ]
    )
    for code, result in zip(LAWD_CODES, results):
        print(f"시군구={code}, 상태={result['status']}, 건수={result['count']}")
    print("거래가 0건인 지역이 있습니다. 지역별 수집 결과를 확인하세요.")


def handle_xml_error():
    raise AirflowException(
        "XML 파싱에 실패한 지역이 있습니다. 수집 로그를 확인하세요."
    )


with DAG(
    dag_id="bronze_realestate_collect",
    start_date=pendulum.datetime(2024, 11, 1, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2024, 12, 31, tz="Asia/Seoul"),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    max_active_tasks=6,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["homework", "Q1", "bronze"],
) as dag:

    with TaskGroup(group_id="collect_group") as collect_group:
        for code in LAWD_CODES:
            PythonOperator(
                task_id=f"collect_{code}",
                python_callable=collect_region,
                op_kwargs={"lawd_cd": code},
            )

    branch = BranchPythonOperator(
        task_id="branch_after_collect",
        python_callable=choose_branch,
    )

    success = EmptyOperator(task_id="collect_success")

    empty = PythonOperator(
        task_id="handle_empty",
        python_callable=handle_empty,
    )

    xml_error = PythonOperator(
        task_id="handle_xml_error",
        python_callable=handle_xml_error,
        retries=0,
    )

    collect_group >> branch >> [success, empty, xml_error]