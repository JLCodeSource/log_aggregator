"""
Module Name: view.py
Created: 2022-08-08
Creator: JL
Change Log: 2022-08-08 - initial commit
Summary: view displays the output of any find requests
Functions: display_results
"""

import logging
from datetime import datetime

from beanie import PydanticObjectId

from aggregator.config import Settings, get_settings
from aggregator.model import JavaLog

settings: Settings = get_settings()
logger: logging.Logger = logging.getLogger(__name__)


async def display_result(
    result: list[JavaLog] | JavaLog | None, database: str | None = settings.database
) -> None:
    results: list[JavaLog] = []
    if result is None:
        return None
    elif isinstance(result, JavaLog):
        results.append(result)
    else:
        results = result

    num_logs = len(results)

    logger.info(
        f"Started display_results coroutine for {num_logs} logs" f"from db: {database}"
    )
    headers = (
        "ObjectId\t\t",
        "Node",
        "Severity",
        "JVM",
        "Timestamp",
        "Source",
        "Type",
        "Message",
    )
    out: str = "| "
    for head in headers:
        out = f"{out}{head}\t| "

    out = out.strip()
    out = out + "\n"

    for result in results:
        id: PydanticObjectId | None = result.id
        node: str = result.node
        severity: str = result.severity
        jvm: str | None = result.jvm
        timestamp: datetime = result.datetime
        source: str | None = result.source
        type: str | None = result.type
        message: str = result.message
        out = (
            f"{out}| {id}\t| {node}\t| {severity}\t| {jvm}\t| "
            f"{timestamp}\t| {source}\t| {type}\t| {message}\t|\n"
        )

    print(out)
