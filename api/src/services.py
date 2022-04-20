from typing import Protocol
from .schemas import IngestionStatus


class Database(dict):
    def write(self, status: IngestionStatus):
        self.setdefault(status.created_by, {})[status.id] = status

    def load(self, username: str, ingestion_id: str):
        print(f"{self}")
        print(f"{username=}")
        print(f"{ingestion_id=}")
        try:
            return self[username][ingestion_id]
        except KeyError:
            raise NotInDb("Record not found")


class Queue:
    def insert(self, status: IngestionStatus):
        ...


class NotInDb(Exception):
    ...
