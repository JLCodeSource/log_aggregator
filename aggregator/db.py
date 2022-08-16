"""
Module Name: db.py
Created: 2022-07-24
Creator: JL
Change Log: 2022-07-26 - added environment settings
Summary: db handles the initialization of the database and all db operations
Functions: init, saveLogs
"""
import asyncio
import logging

import beanie
import motor.motor_asyncio
from beanie import PydanticObjectId
from beanie.odm.enums import SortDirection
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import ValidationError  # AnyUrl
from pymongo.errors import InvalidOperation, ServerSelectionTimeoutError
from pymongo.results import InsertManyResult

from aggregator.config import Settings, get_settings
from aggregator.model import JavaLog

logger: logging.Logger = logging.getLogger(__name__)

settings: Settings = get_settings()


async def init(
    database: str = settings.database, connection: str = settings.connection
) -> AsyncIOMotorClient:
    logger.info(f"Initializing beanie with {database} using {connection}")
    try:
        client: AsyncIOMotorClient = motor.motor_asyncio.AsyncIOMotorClient(connection)

        await beanie.init_beanie(
            database=client[database],
            document_models=[JavaLog]  # type: ignore
            # TODO: Investigate mypy issue
        )
        logger.info(f"Initialized beanie with {database} using {connection}")
    except ServerSelectionTimeoutError as err:
        logger.fatal(
            "ServerSelectionTimeoutError: Server was unreachable " "within the timeout"
        )
        raise err
    logger.info(
        f"Completed initialization of beanie with {database} " f"using {connection}"
    )
    return client


async def insert_logs(
    logs: list | None = None, database: str | None = settings.database
) -> InsertManyResult | None:
    if logs is None:
        logger.warning(
            f"Started insert_logs coroutine for {logs} logs into db: " f"{database}"
        )
        logger.warning(
            f"Ending insert_logs coroutine for {logs} logs into db: " f"{database}"
        )
        return None
    num_logs: int = len(logs)
    logger.info(
        f"Started insert_logs coroutine for {num_logs} logs into db: " f"{database}"
    )
    await asyncio.sleep(0)
    try:
        result: InsertManyResult = await JavaLog.insert_many(logs)
        logger.info(f"Inserted {num_logs} logs into db: " f"{database}")
        for log in logs:
            logger.debug(f"Inserted {log}")
        # TODO: Implement BulkWriteError
    except (ServerSelectionTimeoutError, InvalidOperation) as err:
        logger.error(
            f"ErrorType: {type(err)} - coroutine insert_logs for {num_logs} "
            f"logs failed for db: {database}"
        )
        raise err
    finally:
        logger.info(
            f"Ending insert_logs coroutine for {num_logs} logs " f"into db: {database}"
        )

    return result


async def get_log(
    log_id: PydanticObjectId | None, database: str | None = settings.database
) -> JavaLog | None:
    logger.info(f"Starting get_log coroutine for {log_id} from db: " f"{database}")
    try:
        if log_id is None:
            raise ValidationError("Cannot get None log", model=JavaLog)
        result: JavaLog | None = await JavaLog.get(log_id)
        if result:
            logger.info(f"Got {log_id} from db: {database}")
        else:
            logger.info(f"When getting {log_id} from db {database} " f"found {result}")
    except (ValidationError, ServerSelectionTimeoutError) as err:
        logger.error(
            f"Error: {type(err)} - get_log coroutine for "
            f"{log_id} failed for db: {database}"
        )
        raise err
    finally:
        logger.info(f"Ending get_log coroutine for {log_id} from db: " f"{database}")
    return result


async def find_logs(
    query,
    sort: str | list[tuple[str, SortDirection]] | None,
    database: str | None = settings.database,
) -> list[JavaLog]:
    logger.info(
        f"Starting find_logs coroutine for query: {query} "
        f"& sort: {sort} from db: "
        f"{database}"
    )
    if sort is None:
        result: list[JavaLog] = await JavaLog.find(query).to_list()
    else:
        result = await JavaLog.find(query).sort(sort).to_list()
    logger.info(
        f"Found {len(result)} logs in find_logs coroutine for "
        f"query: {query} & sort: {sort} from db: "
        f"{database}"
    )
    logger.info(
        f"Ending find_logs coroutine for query: {query} & sort: {sort} "
        f"from db: {database}"
    )
    return result
