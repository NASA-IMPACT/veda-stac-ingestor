from decimal import Decimal
import json
from typing import TYPE_CHECKING, List

from boto3.dynamodb import conditions
from pydantic import parse_obj_as

from .schemas import Ingestion

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table


class Database:
    def __init__(self, table: "Table"):
        self.table = table

    def write(self, ingestion: Ingestion):
        data = json.loads(ingestion.json(), parse_float=Decimal)
        self.table.put_item(Item=data)

    def fetch_one(self, username: str, ingestion_id: str):
        response = self.table.get_item(
            Key={"created_by": username, "id": ingestion_id},
        )
        try:
            return Ingestion.parse_obj(response["Item"])
        except KeyError:
            raise NotInDb("Record not found")

    def fetch_many(self, status: str):
        data = self.table.query(
            KeyConditionExpression=conditions.Key('status').eq(status)
        )
        return parse_obj_as(List[Ingestion], data['Items'])


class NotInDb(Exception):
    ...
