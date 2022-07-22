from datetime import datetime
import os
import decimal
from typing import TYPE_CHECKING, Any, Dict, Iterator, List

import boto3
from boto3.dynamodb.types import TypeDeserializer
import orjson
import pydantic
from pypgstac.load import Loader, Methods
from pypgstac.db import PgstacDB

from .dependencies import get_settings, get_table
from .schemas import Ingestion, Status

if TYPE_CHECKING:
    from aws_lambda_typing import context as context_, events
    from aws_lambda_typing.events.dynamodb_stream import DynamodbRecord


deserializer = TypeDeserializer()


def convert_decimals_to_float(item: Dict[str, Any]) -> Dict[str, Any]:
    def decimal_to_float(obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        raise TypeError

    return orjson.loads(
        orjson.dumps(
            item,
            default=decimal_to_float,
        )
    )


def get_queued_ingestions(records: List["DynamodbRecord"]) -> Iterator[Ingestion]:
    for record in records:
        # Parse Record
        parsed = {
            k: deserializer.deserialize(v)
            for k, v in record["dynamodb"]["NewImage"].items()
        }
        ingestion = Ingestion.construct(**parsed)
        if ingestion.status == Status.queued:
            yield ingestion


class DbCreds(pydantic.BaseModel):
    username: str
    password: str
    host: str
    port: int
    dbname: str
    engine: str

    @property
    def dsn_string(self) -> str:
        return f"{self.engine}://{self.username}:{self.password}@{self.host}:{self.port}/{self.dbname}"  # noqa


def get_db_credentials(secret_arn: str) -> DbCreds:
    print("Fetching DB credentials...")
    session = boto3.session.Session(region_name=secret_arn.split(":")[3])
    client = session.client(service_name="secretsmanager")
    response = client.get_secret_value(SecretId=secret_arn)
    return DbCreds.parse_raw(response["SecretString"])


def handler(event: "events.DynamoDBStreamEvent", context: "context_.Context"):
    db_creds = get_db_credentials(os.environ["DB_SECRET_ARN"])
    db = PgstacDB(dsn=db_creds.dsn_string, debug=True)
    loader = Loader(db=db)

    ingestions = list(get_queued_ingestions(event["Records"]))
    items = [
        # NOTE: Important to deserialize values to convert decimals to floats
        convert_decimals_to_float(i.item)
        for i in ingestions
    ]

    if not ingestions:
        print("No queued ingestions to process")
        return

    # Insert into PgSTAC DB
    print(f"Ingesting {len(ingestions)} items")
    batch_status = {
        "status": Status.succeeded,
        "updated_at": datetime.now(),
    }
    try:
        loader.load_items(
            file=items,
            # use insert_ignore to avoid overwritting existing items or upsert to replace
            insert_mode=Methods.insert_ignore,
        )
    except Exception as e:
        batch_status["status"] = Status.failed
        batch_status["message"] = str(e)
        print(e)

    # Update records in DynamoDB
    print("Updating ingested items status in DynamoDB...")
    table = get_table(get_settings())
    with table.batch_writer(overwrite_by_pkeys=["created_by", "id"]) as batch:
        for ingestion in ingestions:
            item = ingestion.copy(update=batch_status).dynamodb_dict()
            batch.put_item(Item=item)
    print("Completed batch...")
