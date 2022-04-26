from unittest.mock import MagicMock
import json

import pytest
from fastapi.testclient import TestClient
from stac_pydantic import Item


@pytest.fixture
def app():
    from src.main import app

    return app


@pytest.fixture
def api_client(app):
    return TestClient(app)


@pytest.fixture
def mock_table(app):
    from src import dependencies

    mock_table = MagicMock()
    app.dependency_overrides[dependencies.get_table] = lambda: mock_table
    return mock_table


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
                "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "title": "3-Band Visual",
                "roles": ["visual"],
            },
            "thumbnail": {
                "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2.jpg",
                "title": "Thumbnail",
                "type": "image/jpeg",
                "roles": ["thumbnail"],
            },
        },
    }


@pytest.fixture
def example_ingestion(example_stac_item):
    from src import schemas

    return json.loads(
        schemas.Ingestion(
            id=example_stac_item["id"],
            created_by="test-user",
            status=schemas.Status.queued,
            item=Item.parse_obj(example_stac_item),
        ).json(by_alias=True)
    )
