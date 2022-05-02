from __future__ import barry_as_FLUFL
from cmath import exp
from datetime import timedelta
import json
import base64
from typing import TYPE_CHECKING, Any, Dict

from boto3.dynamodb import conditions


if TYPE_CHECKING:
    from src import schemas, services
    from fastapi.testclient import TestClient

ingestion_endpoint = "/ingestions"


def test_list(
    api_client: "TestClient",
    mock_table: "services.Table",
    example_ingestion: "schemas.Ingestion",
):
    mock_table.put_item(Item=example_ingestion.dynamodb_dict())
    response = api_client.get(ingestion_endpoint)
    assert response.status_code == 200
    assert response.json() == {
        "items": [json.loads(example_ingestion.json(by_alias=True))],
        "next": None,
    }


def test_list_next_response(
    api_client: "TestClient",
    mock_table: "services.Table",
    example_ingestion: "schemas.Ingestion",
):
    example_ingestions = []
    for i in range(100):
        ingestion = example_ingestion.copy()
        ingestion.id = str(i)
        ingestion.created_at = ingestion.created_at + timedelta(hours=i)
        mock_table.put_item(Item=ingestion.dynamodb_dict())
        example_ingestions.append(ingestion)

    limit = 25
    response = api_client.get(ingestion_endpoint, params={"limit": limit})
    assert response.status_code == 200
    expected_next = json.loads(
        example_ingestions[limit - 1].json(
            include={"created_by", "id", "status", "created_at"}
        )
    )

    assert json.loads(base64.b64decode(response.json()["next"])) == expected_next
    assert response.json()["items"] == [
        json.loads(ingestion.json(by_alias=True))
        for ingestion in example_ingestions[:limit]
    ]
