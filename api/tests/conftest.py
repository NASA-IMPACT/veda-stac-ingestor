import datetime
import os
from typing import Generator

import boto3
import psycopg
import pytest
from fastapi.testclient import TestClient
from moto import mock_dynamodb, mock_ssm
from pypgstac.db import PgstacDB
from pystac import Collection, Extent, SpatialExtent, TemporalExtent
from src.schemas import DashboardCollection
from stac_pydantic import Item


@pytest.fixture
def test_environ():
    # Mocked AWS Credentials for moto (best practice recommendation from moto)
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

    # Config mocks
    os.environ["CLIENT_ID"] = "fake_client_id"
    os.environ["CLIENT_SECRET"] = "fake_client_secret"
    os.environ["DATA_ACCESS_ROLE"] = "arn:aws:iam::123456789012:role/test-role"
    os.environ["DYNAMODB_TABLE"] = "test_table"
    os.environ["JWKS_URL"] = "https://test-jwks.url"
    os.environ["STAC_URL"] = "https://test-stac.url"
    os.environ["RASTER_URL"] = "https://test-raster.url"
    os.environ["USERPOOL_ID"] = "fake_id"
    os.environ["STAGE"] = "testing"
    os.environ["PATH_PREFIX"] = "/api/ingest"


@pytest.fixture
def mock_ssm_parameter_store():
    with mock_ssm():
        yield boto3.client("ssm")


@pytest.fixture
def app(test_environ, mock_ssm_parameter_store):
    from src.main import app

    return app


@pytest.fixture
def api_client(app):
    return TestClient(app)


@pytest.fixture
def mock_table(app, test_environ):
    from src import dependencies, main

    with mock_dynamodb():
        client = boto3.resource("dynamodb")
        mock_table = client.create_table(
            TableName=main.settings.dynamodb_table,
            AttributeDefinitions=[
                {"AttributeName": "created_by", "AttributeType": "S"},
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
                {"AttributeName": "created_at", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "created_by", "KeyType": "HASH"},
                {"AttributeName": "id", "KeyType": "RANGE"},
            ],
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "status",
                    "KeySchema": [
                        {"AttributeName": "status", "KeyType": "HASH"},
                        {"AttributeName": "created_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        app.dependency_overrides[dependencies.get_table] = lambda: mock_table
        yield mock_table
        app.dependency_overrides.pop(dependencies.get_table)


@pytest.fixture
def example_stac_item():
    return {
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "type": "Feature",
        "id": "20201211_223832_CS2",
        "bbox": [
            172.91173669923782,
            1.3438851951615003,
            172.95469614953714,
            1.3690476620161975,
        ],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [172.91173669923782, 1.3438851951615003],
                    [172.95469614953714, 1.3438851951615003],
                    [172.95469614953714, 1.3690476620161975],
                    [172.91173669923782, 1.3690476620161975],
                    [172.91173669923782, 1.3438851951615003],
                ]
            ],
        },
        "properties": {"datetime": "2020-12-11T22:38:32.125000Z"},
        "collection": "simple-collection",
        "links": [
            {
                "rel": "collection",
                "href": "./collection.json",
                "type": "application/json",
                "title": "Simple Example Collection",
            },
            {
                "rel": "root",
                "href": "./collection.json",
                "type": "application/json",
                "title": "Simple Example Collection",
            },
            {
                "rel": "parent",
                "href": "./collection.json",
                "type": "application/json",
                "title": "Simple Example Collection",
            },
        ],
        "assets": {
            "visual": {
                "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2.tif",  # noqa
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "title": "3-Band Visual",
                "roles": ["visual"],
            },
            "thumbnail": {
                "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2.jpg",  # noqa
                "title": "Thumbnail",
                "type": "image/jpeg",
                "roles": ["thumbnail"],
            },
        },
    }


@pytest.fixture
def dashboard_collection() -> DashboardCollection:
    collection = Collection(
        "test-collection",
        "A test collection",
        Extent(
            SpatialExtent(
                [[-180, -90, 180, 90]],
            ),
            TemporalExtent([[datetime.datetime.utcnow(), None]]),
        ),
    )
    return DashboardCollection.parse_obj(collection.to_dict())


@pytest.fixture
def example_ingestion(example_stac_item):
    from src import schemas

    return schemas.Ingestion(
        id=example_stac_item["id"],
        created_by="test-user",
        status=schemas.Status.queued,
        item=Item.parse_obj(example_stac_item),
    )


@pytest.fixture
def pgstac() -> Generator[PgstacDB, None, None]:
    dsn = "postgresql://username:password@localhost:5432/postgis"
    try:
        psycopg.connect(dsn)
    except Exception:
        pytest.skip(f"could not connect to pgstac database: {dsn}")
    with PgstacDB(dsn, commit_on_exit=False) as db:
        yield db
