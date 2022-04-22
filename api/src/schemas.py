from datetime import datetime
import enum
from urllib.parse import urlparse
from typing import Dict, Optional, TYPE_CHECKING, TypedDict

from pydantic import BaseModel, validator
from stac_pydantic import Item, shared

from . import validators

if TYPE_CHECKING:
    from . import services


class AccessibleAsset(shared.Asset):
    @validator("href")
    def is_accessible(cls, href):
        url = urlparse(href)

        if url.scheme in ["https", "http"]:
            validators.url_is_accessible(href)
        elif url.scheme in ["s3"]:
            validators.s3_object_is_accessible(
                bucket=url.hostname, key=url.path.lstrip("/")
            )
        else:
            ValueError(f"Unsupported scheme: {url.scheme}")

        return href


class AccessibleItem(Item):
    assets: Dict[str, AccessibleAsset]

    @validator("collection")
    def exists(cls, collection):
        # TODO: Validate that collection exists
        print(f"{collection=}")


class Status(str, enum.Enum):
    queued = "queued"
    failed = "failed"
    succeeded = "succeeded"
    cancelled = "cancelled"


class Ingestion(BaseModel):
    id: str
    created_at: datetime = None
    updated_at: datetime = None
    created_by: str

    item: Item
    status: Status
    message: Optional[str]

    @validator("created_at", pre=True, always=True, allow_reuse=True)
    @validator("updated_at", pre=True, always=True, allow_reuse=True)
    def set_ts_now(cls, v):
        return v or datetime.now()

    def insert_into_queue(self, db: "services.Database"):
        self.status = Status.queued
        return self.save(db)

    def delete_from_queue(self, db: "services.Database"):
        self.status = Status.cancelled
        return self.save(db)

    def save(self, db: "services.Database"):
        self.updated_at = datetime.now()
        db.write(self)
        return self


class S3Details(BaseModel):
    bucket: str
    prefix: str


class AwsCredentials(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str


class TemporaryCredentials(BaseModel):
    s3: S3Details
    credentials: AwsCredentials
