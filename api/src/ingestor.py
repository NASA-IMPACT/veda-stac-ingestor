from datetime import datetime
import os
import decimal
import traceback
from typing import TYPE_CHECKING, Iterator, List, Optional, Sequence


import boto3
from boto3.dynamodb.types import TypeDeserializer
import ddbcereal
from pypgstac.db import PgstacDB

from .auth import get_settings
from .dependencies import get_table
from .schemas import Ingestion, Status
from .utils import (
    IngestionType,
    get_db_credentials,
    convert_decimals_to_float,
    load_into_pgstac,
)

if TYPE_CHECKING:
    from aws_lambda_typing import context as context_, events
    from aws_lambda_typing.events.dynamodb_stream import DynamodbRecord


# Hack to avoid issues deserializing large values
# https://github.com/boto/boto3/issues/2500#issuecomment-654925049
boto3.dynamodb.types.DYNAMODB_CONTEXT = decimal.Context(prec=100)
# Inhibit Inexact Exceptions
boto3.dynamodb.types.DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
# Inhibit Rounded Exceptions
boto3.dynamodb.types.DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0


def get_queued_ingestions(records: List["DynamodbRecord"]) -> Iterator[Ingestion]:
    """
    Get stream of ingestions that have been queue in the dynamodb database
    """
    deserializer = TypeDeserializer()
    for record in records:
        # Parse Record
        try:
            parsed = {
                k: deserializer.deserialize(v)
                for k, v in record["dynamodb"]["NewImage"].items()
            }
        except decimal.Rounded:
            print("Decimal rounding error - using alternate deserializer")
            # The above hack doesn't cover all cases
            # ddbcereal can, but is slower and has less eyes on its codebase than boto.
            alt_deserializer = ddbcereal.deserializer()
            parsed = {
                k: alt_deserializer.deserialize(v)
                for k, v in record["dynamodb"]["NewImage"].items()
            }
        ingestion = Ingestion.construct(**parsed)
        if ingestion.status == Status.queued:
            yield ingestion


def update_dynamodb(
    ingestions: Sequence[Ingestion],
    status: Status,
    message: Optional[str] = None,
):
    """
    Bulk update DynamoDB with ingestion results.
    """
    # Update records in DynamoDB
    print(f"Updating ingested items status in DynamoDB, marking as {status}...")
    table = get_table(get_settings())
    with table.batch_writer(overwrite_by_pkeys=["created_by", "id"]) as batch:
        for ingestion in ingestions:
            batch.put_item(
                Item=ingestion.copy(
                    update={
                        "status": status,
                        "message": message,
                        "updated_at": datetime.now(),
                    }
                ).dynamodb_dict()
            )


def handler(event: "events.DynamoDBStreamEvent", context: "context_.Context"):
    # Parse input
    ingestions = list(get_queued_ingestions(event["Records"]))
    if not ingestions:
        print("No queued ingestions to process")
        return

    items = [
        # NOTE: Important to deserialize values to convert decimals to floats
        convert_decimals_to_float(ingestion.item)
        for ingestion in ingestions
    ]

    creds = get_db_credentials(os.environ["DB_SECRET_ARN"])

    # Insert into PgSTAC DB
    outcome = Status.succeeded
    message = None
    try:
        with PgstacDB(dsn=creds.dsn_string, debug=True) as db:
            load_into_pgstac(
                db=db,
                ingestions=items,
                table=IngestionType.items,
            )
    except Exception as e:
        traceback.print_exc()
        print(f"Encountered failure loading items into pgSTAC: {e}")
        outcome = Status.failed
        message = str(e)

    # Update DynamoDB with outcome
    update_dynamodb(
        ingestions=ingestions,
        status=outcome,
        message=message,
    )

    print("Completed batch...")
