import base64
import binascii
import enum
import json
import re
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Literal, Union
from typing_extensions import Annotated
from urllib.parse import urlparse

from fastapi.exceptions import RequestValidationError
from pydantic import (
    BaseModel,
    PositiveInt,
    dataclasses,
    error_wrappers,
    validator,
    root_validator,
    Field,
    Extra,
    ValidationError,
)

from stac_pydantic import Item, Collection, shared


from . import validators
from .schema_helpers import DatetimeExtent, BboxExtent, TemporalExtent

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
        validators.collection_exists(collection_id=collection)
        return collection


class DashboardCollection(Collection):
    dashboard_is_periodic: bool
    dashboard_time_density: Literal["day", "month", "year", "null"] = Field(
        default="null"
    )
    item_assets: Dict
    extent: DatetimeExtent

    @validator("item_assets")
    def cog_default_exists(cls, item_assets):
        validators.cog_default_exists(item_assets=item_assets)
        return item_assets


class Status(str, enum.Enum):
    @classmethod
    def _missing_(cls, value):
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        return cls.unknown

    started = "started"
    queued = "queued"
    failed = "failed"
    succeeded = "succeeded"
    cancelled = "cancelled"


class BaseResponse(BaseModel):
    id: str
    status: Status


class ExecutionResponse(BaseResponse):
    message: str
    discovered_files: List[str]


class Ingestion(BaseModel):
    id: str
    status: Status
    message: Optional[str]
    created_by: str
    created_at: datetime = None
    updated_at: datetime = None

    item: Item

    @validator("created_at", pre=True, always=True, allow_reuse=True)
    @validator("updated_at", pre=True, always=True, allow_reuse=True)
    def set_ts_now(cls, v):
        return v or datetime.now()

    def enqueue(self, db: "services.Database"):
        self.status = Status.queued
        return self.save(db)

    def cancel(self, db: "services.Database"):
        self.status = Status.cancelled
        return self.save(db)

    def save(self, db: "services.Database"):
        self.updated_at = datetime.now()
        db.write(self)
        return self

    def dynamodb_dict(self, by_alias=True):
        """DynamoDB-friendly serialization"""
        return json.loads(self.json(by_alias=by_alias), parse_float=Decimal)


@dataclasses.dataclass
class ListIngestionRequest:
    status: Status = Status.queued
    limit: PositiveInt = None
    next: Optional[str] = None

    def __post_init_post_parse__(self) -> None:
        # https://github.com/tiangolo/fastapi/issues/1474#issuecomment-1049987786
        if self.next is None:
            return

        try:
            self.next = json.loads(base64.b64decode(self.next))
        except (UnicodeDecodeError, binascii.Error):
            raise RequestValidationError(
                [
                    error_wrappers.ErrorWrapper(
                        ValueError(
                            "Unable to decode next token. Should be base64 encoded JSON"
                        ),
                        "query.next",
                    )
                ]
            )


class ListIngestionResponse(BaseModel):
    items: List[Ingestion]
    next: Optional[str]

    @validator("next", pre=True)
    def b64_encode_next(cls, next):
        """
        Base64 encode next parameter for easier transportability
        """
        if isinstance(next, dict):
            return base64.b64encode(json.dumps(next).encode())
        return next


class UpdateIngestionRequest(BaseModel):
    status: Status = None
    message: str = None


class WorkflowInputBase(BaseModel):
    collection: str = ""
    upload: Optional[bool] = False
    cogify: Optional[bool] = False
    dry_run: bool = False

    @validator("collection")
    def exists(cls, collection):
        validators.collection_exists(collection_id=collection)
        return collection


class S3Input(WorkflowInputBase):
    discovery: Literal["s3"]
    prefix: str
    bucket: str
    filename_regex: str
    datetime_range: Optional[str]
    start_datetime: Optional[datetime]
    end_datetime: Optional[datetime]
    single_datetime: Optional[datetime]

    @root_validator
    def is_accessible(cls, values):
        bucket, prefix = values.get("bucket"), values.get("prefix")
        validators.s3_bucket_object_is_accessible(bucket=bucket, prefix=prefix)
        return values


class CmrInput(WorkflowInputBase):
    discovery: Literal["cmr"]
    version: Optional[str]
    include: Optional[str]
    temporal: Optional[List[datetime]]
    bounding_box: Optional[List[float]]


# allows the construction of models with a list of discriminated unions
ItemUnion = Annotated[Union[S3Input, CmrInput], Field(discriminator="discovery")]


class Dataset(BaseModel):
    collection: str
    title: str
    description: str
    license: str
    dashboard_is_periodic: bool
    dashboard_time_density: str
    spatial_extent: BboxExtent
    temporal_extent: TemporalExtent
    sample_files: List[str]  # unknown how this will work with CMR
    discovery_items: List[ItemUnion]

    class Config:
        extra = Extra.allow

    @root_validator
    def check_time_density(cls, v):
        if v["dashboard_is_periodic"] and v["dashboard_time_density"] not in [
            "month",
            "day",
            "year",
        ]:
            raise ValueError("Invalid time density")
        if not v["dashboard_is_periodic"] and v["dashboard_time_density"] != "null":
            raise ValueError("Invalid time density")
        return v

    # collection id must be all lowercase, with optional - delimiter
    @validator("collection")
    def check_id(cls, v):
        if not re.match(r"[a-z]+(?:-[a-z]+)*", v):
            raise ValueError("Invalid id")
        return v

    # all sample files must begin with prefix and their last element must match regex
    @root_validator
    def check_sample_files(cls, v):
        if "s3" not in [item.discovery for item in v["discovery_items"]]:
            print("No s3 discovery items to validate sample files against")
            return v
        # TODO cmr handling/validation
        valid_matches = []
        for item in v["discovery_items"]:
            if item.discovery == "s3":
                valid_matches.append(
                    {"prefix": item.prefix, "regex": item.filename_regex}
                )
        for fname in v["sample_files"]:
            if not any([fname.startswith(match["prefix"]) for match in valid_matches]):
                raise ValidationError(
                    f"Invalid sample file - {fname} doesn't match prefix"
                )
            if not any(
                [
                    re.search(match["regex"], fname.split("/")[-1])
                    for match in valid_matches
                ]
            ):
                raise ValidationError(
                    f"Invalid sample file - {fname} doesn't match provided regex"
                )
        return v
