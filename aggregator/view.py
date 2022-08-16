"""
Module Name: view.py
Created: 2022-08-08
Creator: JL
Change Log: 2022-08-08 - initial commit
Summary: view displays the output of any find requests
Functions: display_results
"""
from aggregator.model import JavaLog
from aggregator.config import Settings, get_settings
from beanie import PydanticObjectId
import logging

settings: Settings = get_settings()
logger: logging.Logger = logging.getLogger(__name__)


async def display_result(result: list[JavaLog] | JavaLog | None,
                         database: str | None = settings.database) -> None:
    results: list[JavaLog] = []
    if result is None:
        return None
    elif isinstance(result, JavaLog):
        results.append(result)
    else:
        results: list[JavaLog] = result

    num_logs = len(results)

    logger.info(f"Started display_results coroutine for {num_logs} logs"
                f"from db: {database}")
    headers = (
        "ObjectId\t\t", "Node", "Severity", "JVM",
        "Timestamp", "Source", "Type", "Message"
    )
    out: str = "| "
    for head in headers:
        out: str = f"{out}{head}\t| "

    out: str = out.strip()
    out: str = out + "\n"

    for result in results:
        id: PydanticObjectId | None = result.id
        node = result.node
        severity = result.severity
        jvm = result.jvm
        timestamp = result.datetime
        source = result.source
        type = result.type
        message = result.message
        out = (
            f"{out}| {id}\t| {node}\t| {severity}\t| {jvm}\t| "
            f"{timestamp}\t| {source}\t| {type}\t| {message}\t|\n"
        )

    print(out)
