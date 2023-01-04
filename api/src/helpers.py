import json
from pathlib import Path

import boto3

from typing import Dict, Union
from uuid import uuid4

from pydantic.tools import parse_obj_as
import requests


try:
    from .schemas import Status, BaseResponse, ExecutionResponse
except ImportError:
    from schemas import Status, BaseResponse, ExecutionResponse

EXECUTION_NAME_PREFIX = "workflows-api"


def trigger_discover(input: Dict, data_pipeline_arn: str) -> Dict:
    """
    Trigger a discover event.
    """
    unique_key = str(uuid4())

    client = boto3.client("stepfunctions")
    client.start_execution(
        stateMachineArn=data_pipeline_arn,
        name=f"{EXECUTION_NAME_PREFIX}-{unique_key}",
        input=input.json(),
    )
    return BaseResponse(
        **{
            "id": unique_key,
            "status": Status.started,
        }
    )


def execute_dag(env_name: str, input: Dict, dag_id: str) -> requests.Response:
    assert dag_id in ("veda_discover", "veda_ingest")

    client = boto3.client("mwaa")
    token = client.create_cli_token(Name=env_name)
    url = f"https://{token['WebServerHostname']}/aws_maa/cli"
    headers = {
        "Authorization": f"Bearer {token['CliToken']}",
        "Content-Type": "text/plain"
    }
    body = f"dags trigger {dag_id} -c '{json.dumps(input)}'" # input comes from veda-data-pipelines/data/step_functions_inputs/*.json ?

    _res = requests.post(url, data=body, headers=headers)
    unique_key = str(uuid4())
    return BaseResponse(
        **{
            "id": unique_key,
            "status": Status.started,
        }
    )


def trigger_discovery(env_name: str, input: Dict) -> requests.Response:
    return execute_dag(env_name, input, "veda_discover")


def _build_execution_arn(id: str, data_pipeline_arn: str) -> str:
    """
    Build the execution arn from an id
    """
    arn_prefix = data_pipeline_arn.replace(":stateMachine:", ":execution:")
    return f"{arn_prefix}:{EXECUTION_NAME_PREFIX}-{id}"


def get_status(id: str, data_pipeline_arn: str) -> Dict:
    """
    Get the status of a workflow execution.
    """

    def _find_discovery_success_event(events):
        DISCOVERY_LAMBDA_SUFFIX = "lambda-s3-discovery-fn"
        # Assumption: All the task events exists in a successful step function execution
        # Get the discovery scheduled event id
        discovery_scheduled_id = next(
            (
                event["id"]
                for event in events
                if (event["type"] == "TaskScheduled")
                and (
                    params := json.loads(
                        event["taskScheduledEventDetails"]["parameters"]
                    )
                )
                and (params.get("FunctionName", "").endswith(DISCOVERY_LAMBDA_SUFFIX))
            ),
            None,
        )
        # Get the first succeeded event after the discovery scheduled event,
        # this will be the discovery success event
        return (
            next(
                (
                    event
                    for event in events[discovery_scheduled_id - 1 :]
                    if event["type"] == "TaskSucceeded"
                ),
                None,
            )
            if discovery_scheduled_id
            else None
        )

    client = boto3.client("stepfunctions")
    execution_arn = _build_execution_arn(id, data_pipeline_arn)
    try:
        response = client.describe_execution(executionArn=execution_arn)
    except (client.exceptions.ExecutionDoesNotExist, client.exceptions.InvalidArn):
        return BaseResponse(
            **{
                "id": id,
                "status": Status.nonexistent,
            }
        )

    status = response["status"]
    extras = {}
    if status == "SUCCEEDED":
        exec_history = client.get_execution_history(executionArn=execution_arn)
        events = exec_history.get("events")
        event = _find_discovery_success_event(events)
        if event:
            payload = json.loads(event["taskSucceededEventDetails"]["output"])[
                "Payload"
            ]
            files = [Path(obj["s3_filename"]).stem for obj in payload["objects"]]
            cogify = payload["cogify"]

            extras = {
                "discovered_files": files,
                "message": f"Files queued to {'cogify' if cogify else 'stac-ready'} queue",  # noqa
            }

    return parse_obj_as(
        Union[ExecutionResponse, BaseResponse],
        {"id": id, "status": Status(status), **extras},
    )
