import json
from pathlib import Path
from typing import Dict, Union
from uuid import uuid4

import boto3
from pydantic.tools import parse_obj_as

try:
    from .schemas import BaseResponse, ExecutionResponse, Status
except ImportError:
    from schemas import BaseResponse, ExecutionResponse, Status

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
