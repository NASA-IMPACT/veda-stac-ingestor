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
    root_validator,
    Extra,
    ValidationError
)

from stac_pydantic import Item, Collection, shared

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
        validators.collection_exists(collection_id=collection)
        return collection


class DashboardCollection(Collection):
    is_periodic: bool = Field(alias="dashboard:is_periodic")
    time_density: Literal["day", "month", "year", "null"] = Field(
        alias="dashboard:time_density", default="null"
    )
    item_assets: Dict

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


class Discovery(str, enum.Enum):
    s3 = "s3"
    cmr = "cmr"


class WorkflowInputBase(BaseModel):
    collection: str
    discovery: Discovery
    upload: Optional[bool] = False
    cogify: Optional[bool] = False

    @validator("collection")
    def exists(cls, collection):
        validators.collection_exists(collection_id=collection)
        return collection


class S3Input(WorkflowInputBase):
    # s3 discovery
    discovery: Literal[Discovery.s3]

    prefix: str
    bucket: str
    filename_regex: Optional[str]
    start_datetime: Optional[datetime]
    end_datetime: Optional[datetime]
    single_datetime: Optional[datetime]

    @root_validator
    def is_accessible(cls, values):
        bucket, prefix = values.get("bucket"), values.get("prefix")
        validators.s3_bucket_object_is_accessible(bucket=bucket, prefix=prefix)
        return values


class CmrInput(WorkflowInputBase):
    # cmr discovery
    discovery: Literal[Discovery.cmr]

    version: Optional[str]
    include: Optional[str]
    temporal: Optional[List[datetime]]
    bounding_box: Optional[List[float]]
# TODO we want these validations but I also want to use the SpatialExtent and TemporalExtent models provided by stac_pydantic
class Extent(BaseModel):
    xmin: float
    ymin: float
    xmax: float
    ymax: float
    startdate: datetime
    enddate: datetime
    
    @root_validator
    def check_extent(cls, v):
        # mins must be below maxes
        if v['xmin'] >= v['xmax'] or v['ymin'] >= v['ymax']:
            raise ValueError('Invalid extent')
        # ys must be within -90 and 90, x between -180 and 180
        if v['xmin'] < -180 or v['xmax'] > 180 or v['ymin'] < -90 or v['ymax'] > 90:
            raise ValueError('Invalid extent')
        return v

# Not to be confused with the stac_pydantic Item model - these define the inputs for the `insert-item` workflows
class Item(BaseModel):
    collection: Optional[str]
    cogify: bool = False
    upload: bool = False
    dry_run: bool = False

class s3Item(Item):
    discovery: Literal['s3']
    # for s3
    prefix: str
    bucket: str
    filename_regex : str
    datetime_range: Optional[str] # literal (month, day, year)
    start_datetime: datetime
    end_datetime: datetime

class cmrItem(Item):
    discovery: Literal['cmr']
    # for cmr
    version: str
    temporal : List[str]
    bounding_box: str
    include: str

# not a great name, but allows the construction of models with a list of discriminated unions
ItemUnion = Annotated[Union[s3Item, cmrItem], Field(discriminator='discovery')]

class Dataset(BaseModel):
    collection: str
    title: str
    description: str
    license: str
    dashboard_is_periodic: bool
    dashboard_time_density: str
    extent: Extent
    sample_files: List[str] # TODO how to do with CMR?
    discovery_items : List[ItemUnion]

    class Config:
        extra = Extra.allow

    @validator('license')
    def check_license(cls, v):
        # value must be one of: CC0 MIT
        # TODO fill in rest of list
        if v not in ['CC0', 'MIT']:
            raise ValueError('Invalid license')
        return v
    
    # time density must be one of month, day, year if periodic is true, otherwise it must be null
    @root_validator
    def check_time_density(cls, v):
        if v['dashboard_is_periodic'] and v['dashboard_time_density'] not in ['month', 'day', 'year']:
            raise ValueError('Invalid time density')
        if not v['dashboard_is_periodic'] and v['dashboard_time_density'] != 'null':
            raise ValueError('Invalid time density')
        return v
    
    # collection id must be all lowercase, with optional - delimiter
    @validator('collection')
    def check_id(cls, v):
        if not re.match(r'[a-z]+(?:-[a-z]+)*', v):
            raise ValueError('Invalid id')
        return v

    @validator("collection")
    def exists(cls, v):
        validators.collection_exists(collection_id=v)
        return v

    # all sample files must begin with prefix and their last element must match regex
    @root_validator
    def check_sample_files(cls, v):
        if 's3' not in [item.discovery for item in v['discovery_items']]:
            print('No s3 discovery items to validate sample files against')
            return v
        # TODO cmr handling/validation
        valid_matches = []
        for item in v['discovery_items']:
            if item.discovery == 's3':
                valid_matches.append(
                    {
                        'prefix': item.prefix,
                        'regex': item.filename_regex
                    }
                )
        for file in v['sample_files']:
            if not any([file.startswith(match['prefix']) for match in valid_matches]):
                raise ValidationError(f'Invalid sample file - {file} doesn\'t match prefix')
            if not any([re.match(match['regex'], file.split('/')[-1]) for match in valid_matches]):
                raise ValidationError(f'Invalid sample file - {file} doesn\'t match regex')
        return v