"""
Module Name: model.py
Created: 2022-07-24
Creator: JL
Change Log: Initial
Summary: model manages the document (log) schema
Classes: Log, JavaLog
"""

from datetime import datetime
from typing import ClassVar, Optional

import pymongo
from beanie import Document, Indexed


class Log(Document):
    node: Indexed(str)  # type: ignore
    datetime: datetime
    message: Indexed(str, pymongo.DESCENDING)  # type: ignore

    class Settings:
        name: str = "logs"
        anystr_strip_whitespace: bool = True
        indexes: ClassVar[list[list[tuple[str, str]]]] = [
            [
                ("node", pymongo.TEXT),
                ("message", pymongo.TEXT),
            ]
        ]


class JavaLog(Log):
    severity: Indexed(str)  # type: ignore
    jvm: Optional[str] = None
    source: Optional[Indexed(str)] = None  # type: ignore
    type: Optional[Indexed(str)] = None  # type: ignore

    class Settings:
        name: str = "javalogs"

    indexes: ClassVar[list[list[tuple[str, str]]]] = [
        [
            ("node", pymongo.TEXT),
            ("message", pymongo.TEXT),
            ("severity", pymongo.TEXT),
            ("source", pymongo.TEXT),
            ("type", pymongo.TEXT),
        ]
    ]
