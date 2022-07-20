from datetime import datetime
import os
import json
from typing import TYPE_CHECKING, Iterator, List

import boto3
from boto3.dynamodb.types import TypeDeserializer
import pydantic
from pypgstac.load import Loader, Methods
from pypgstac.db import PgstacDB
from stac_pydantic import Item

from .dependencies import get_settings, get_table
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

    # Insert into PgSTAC DB
    print(f"Ingesting {len(ingestions)} items")
    loader.load_items(
        file=[json.loads(Item.parse_obj(i.item).json()) for i in ingestions],
        # use insert_ignore to avoid overwritting existing items or upsert to replace
        insert_mode=Methods.insert_ignore,
    )

    # Update records in DynamoDB
    table = get_table(get_settings())
    with table.batch_writer() as batch:
        for ingestion in ingestions:
            batch.put_item(
                Item=ingestion.copy(
                    update={
                        "status": Status.succeeded,
                        "updated_at": datetime.now(),
                    }
                ).dynamodb_dict()
            )

    return {"statusCode": 200, "body": "Done"}
