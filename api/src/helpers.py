import base64
import os
from typing import Dict
from uuid import uuid4

import boto3
import requests

try:
    from .schemas import BaseResponse, Status
except ImportError:
    from schemas import BaseResponse, Status

EXECUTION_NAME_PREFIX = "workflows-api"


def trigger_discover(input: Dict) -> Dict:
    MWAA_ENV = os.environ["MWAA_ENV"]
    airflow_client = boto3.client("mwaa")
    mwaa_cli_token = airflow_client.create_cli_token(Name=MWAA_ENV)

    mwaa_webserver_hostname = (
        f"https://{mwaa_cli_token['WebServerHostname']}/aws_mwaa/cli"
    )

    unique_key = str(uuid4())
    raw_data = f"dags trigger veda_discover --conf '{input.json()}' -r {unique_key}"
    mwaa_response = requests.post(
        mwaa_webserver_hostname,
        headers={
            "Authorization": "Bearer " + mwaa_cli_token["CliToken"],
            "Content-Type": "application/json",
        },
        data=raw_data,
    )
    if mwaa_response.status_code not in [200, 201]:
        raise Exception(
            f"Failed to trigger airflow: {mwaa_response.status_code} {mwaa_response.text}"
        )
    else:
        return BaseResponse(
            **{
                "id": unique_key,
                "status": Status.started,
            }
        )


def get_status(dag_run_id: str) -> Dict:
    """
    Get the status of a workflow execution.
    """
    MWAA_ENV = os.environ["MWAA_ENV"]
    airflow_client = boto3.client("mwaa")
    mwaa_cli_token = airflow_client.create_cli_token(Name=MWAA_ENV)

    mwaa_webserver_hostname = (
        f"https://{mwaa_cli_token['WebServerHostname']}/aws_mwaa/cli"
    )

    raw_data = "dags list-runs -d veda_discover"
    mwaa_response = requests.post(
        mwaa_webserver_hostname,
        headers={
            "Authorization": "Bearer " + mwaa_cli_token["CliToken"],
            "Content-Type": "application/json",
        },
        data=raw_data,
    )
    decoded_response = base64.b64decode(mwaa_response.json()["stdout"]).decode("utf8")
    rows = decoded_response.split("\n")

    try:
        matched_row = next(row for row in rows if dag_run_id in row)
    except StopIteration:
        raise Exception(f"Failed to find dag run id: {dag_run_id}")

    columns = matched_row.split("|")
    status = columns[2].strip()

    # Statuses in Airflow differ slightly from our own, so we convert them here
    if status == "success":
        run_status = Status.succeeded
    elif status == "failed":
        run_status = Status.failed
    elif status == "running":
        run_status = Status.started
    elif status == "queued":
        run_status = Status.queued

    return BaseResponse(
        **{
            "id": dag_run_id,
            "status": run_status,
        }
    )
