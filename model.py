from pydantic import BaseModel
from beanie import Document, Indexed, init_beanie
from datetime import datetime
import pymongo


class Log(Document):
    node: Indexed(str)
    datetime: datetime
    message: Indexed(str, pymongo.DESCENDING)

    class Settings:
        name = "logs"
        anystr_strip_whitespace = True
        indexes = [
            [
                ("node", pymongo.TEXT),
                ("message", pymongo.TEXT),
            ]
        ]


class JavaLog(Log):
    severity: Indexed(str)
    jvm: str
    source: Indexed(str)
    type: Indexed(str)

    class Settings:
        name = "javalogs"
    indexes = [
        [
            ("node", pymongo.TEXT),
            ("message", pymongo.TEXT),
            ("severity", pymongo.TEXT),
            ("source", pymongo.TEXT),
            ("type", pymongo.TEXT),
        ]
    ]
