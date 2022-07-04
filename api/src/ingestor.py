from datetime import datetime
import os
from typing import TYPE_CHECKING, Iterator, List

import boto3
from boto3.dynamodb.types import TypeDeserializer
import pydantic
from pypgstac.load import Loader, Methods
from pypgstac.db import PgstacDB

from api.src.dependencies import get_table

from .schemas import Ingestion, Status

if TYPE_CHECKING:
    from aws_lambda_typing import context as context_, events
    from aws_lambda_typing.events.dynamodb_stream import DynamodbRecord


deserializer = TypeDeserializer()


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
    session = boto3.session.Session(region_name=secret_arn.split(":")[3])
    client = session.client(service_name="secretsmanager")
    response = client.get_secret_value(SecretId=secret_arn)
    return DbCreds.parse_raw(response["SecretString"])


def handler(event: "events.DynamoDBStreamEvent", context: "context_.Context"):
    db_creds = get_db_credentials(os.environ["DB_SECRET_ARN"])
    db = PgstacDB(dsn=db_creds.dsn_string, debug=True)
    loader = Loader(db=db)

    ingestions = list(get_queued_ingestions(event["Records"]))

    # Insert into PgSTAC DB
    loader.load_items(
        file=[i.item for i in ingestions],
        # use insert_ignore to avoid overwritting existing items or upsert to replace
        insert_mode=Methods.insert_ignore,
    )

    # Update records in DynamoDB
    with get_table().batch_writer() as batch:
        for ingestion in ingestions:
            batch.put_item(
                Item=ingestion.copy(
                    update={
                        "status": Status.succeeded,
                        "updated_at": datetime.now(),
                    }
                ).json()
            )

    return {"statusCode": 200, "body": "Done"}
