import pytest
from pypgstac.db import PgstacDB
from pystac import Collection
from src.collection import Publisher
from src.schemas import DashboardCollection
from src.utils import DbCreds


@pytest.fixture
def publisher() -> Publisher:
    return Publisher(
        DbCreds(
            username="username",
            password="password",
            host="localhost",
            port=5432,
            dbname="postgis",
            engine="postgresql",
        )
    )


def test_ingest(
    pgstac: PgstacDB, publisher: Publisher, dashboard_collection: DashboardCollection
) -> None:
    publisher.ingest(dashboard_collection)
    collection = Collection.from_dict(
        pgstac.query_one(
            r"SELECT * FROM pgstac.get_collection(%s)", [dashboard_collection.id]
        )
    )
    collection.validate()
