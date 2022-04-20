import random
import string

from fastapi import Depends, security

from . import services

authentication = security.HTTPBasic()


def get_username(credentials: security.HTTPBasicCredentials = Depends(authentication)):
    return credentials.username


def get_random_id():
    return "".join(
        random.choices(
            string.ascii_lowercase + string.ascii_uppercase + string.digits, k=12
        )
    )


def get_queue() -> services.Queue:
    return services.Queue()


# TODO: Wire up to actual table
_db = services.Database()


def get_db() -> services.Database:
    return _db
