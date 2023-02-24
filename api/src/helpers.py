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
    run_id = f"{input.collection}_{unique_key}"
    raw_data = f"dags trigger veda_discover --conf '{input.json()}' -r {run_id}"
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
                "id": run_id,
                "status": Status.started,
            }
        )


def convert_status(status: str) -> Status:
    """Converts airflow status strings to our status enum."""
    if status == "success":
        run_status = Status.succeeded
    elif status == "failed":
        run_status = Status.failed
    elif status == "running":
        run_status = Status.started
    elif status == "queued":
        run_status = Status.queued
    else:
        raise Exception(f"Unknown status: {status}")

    return run_status


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

    raw_data_ingest = "dags list-runs -d veda_ingest"
    mwaa_response_ingest = requests.post(
        mwaa_webserver_hostname,
        headers={
            "Authorization": "Bearer " + mwaa_cli_token["CliToken"],
            "Content-Type": "application/json",
        },
        data=raw_data_ingest,
    )
    decoded_response_ingest = base64.b64decode(
        mwaa_response_ingest.json()["stdout"]
    ).decode("utf8")
    rows_ingest = decoded_response_ingest.split("\n")

    raw_data_discover = "dags list-runs -d veda_discover"
    mwaa_response_discover = requests.post(
        mwaa_webserver_hostname,
        headers={
            "Authorization": "Bearer " + mwaa_cli_token["CliToken"],
            "Content-Type": "application/json",
        },
        data=raw_data_discover,
    )
    decoded_response_discover = base64.b64decode(
        mwaa_response_discover.json()["stdout"]
    ).decode("utf8")
    rows_discover = decoded_response_discover.split("\n")

    rows = rows_ingest + rows_discover
    rows_columns = [row.split("|") for row in rows if dag_run_id in row]
    statuses = [convert_status(row[2].strip()) for row in rows_columns]

    if Status.failed in statuses:
        total_status = Status.failed
    elif Status.started in statuses:
        total_status = Status.started
    elif Status.queued in statuses:
        total_status = Status.queued
    else:
        total_status = Status.succeeded

    return BaseResponse(
        **{
            "id": dag_run_id,
            "status": total_status,
        }
    )
