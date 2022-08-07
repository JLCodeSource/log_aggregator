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

import motor
import beanie
from pymongo.errors import ServerSelectionTimeoutError, InvalidOperation
from pymongo.results import InsertManyResult
from pydantic import ValidationError

from aggregator.config import get_settings
from aggregator.model import JavaLog

logger = logging.getLogger(__name__)

settings = get_settings()
client = motor.motor_asyncio.AsyncIOMotorClient(settings.connection)


async def init(database: str = settings.database, client=client):
    logger.info(f"Initializing beanie with {database} using {client}")
    try:
        await beanie.init_beanie(database=client[database],
                                 document_models=[JavaLog])
        logger.info(f"Initialized beanie with {database} using {client}")
    except ServerSelectionTimeoutError as err:
        logger.fatal("ServerSelectionTimeoutError: Server was unreachable "
                     "within the timeout")
        raise err
    logger.info(
        f"Completed initialization of beanie with {database} using {client}")
    return "ok"


async def insert_logs(logs: list | None = None) -> InsertManyResult:
    if logs is None:
        logger.warning(
            f"Started insert_logs coroutine for {logs} logs into db: "
            f"{settings.database}"
        )
        logger.warning(
            f"Ending insert_logs coroutine for {logs} logs into db: "
            f"{settings.database}"
        )
        return None
    num_logs = len(logs)
    logger.info(
        f"Started insert_logs coroutine for {num_logs} logs into db: "
        f"{settings.database}"
    )
    await asyncio.sleep(0)
    try:
        result = await JavaLog.insert_many(logs)
        logger.info(f"Inserted {num_logs} logs into db: "
                    f"{settings.database}")
        for log in logs:
            logger.debug(f"Inserted {log}")
        # TODO: Implement BulkWriteError
    except (ServerSelectionTimeoutError, InvalidOperation) as err:
        logger.error(
            f"ErrorType: {type(err)} - coroutine insert_logs for {num_logs} "
            f"logs failed for db: {settings.database}"
        )
        raise err
    finally:
        logger.info(
            f"Ending insert_logs coroutine for {num_logs} logs "
            f"into db: {settings.database}"
        )

    return result


async def get_log(log_id: JavaLog | None = None):
    logger.info(
        f"Starting get_log coroutine for {log_id} from db: "
        f"{settings.database}"
    )
    try:
        result = await JavaLog.get(log_id)
        if result:
            logger.info(f"Got {log_id} from db: {settings.database}")
        else:
            logger.info(f"When getting {log_id} from db {settings.database} "
                        f"found {result}")
    except (ValidationError, ServerSelectionTimeoutError) as err:
        logger.error(f"Error: {type(err)} - get_log coroutine for "
                     f"{log_id} failed for db: {settings.database}")
        raise err
    finally:
        logger.info(
            f"Ending get_log coroutine for {log_id} from db: "
            f"{settings.database}"
        )
    return result


async def find_logs(query, sort=None) -> list[JavaLog]:
    logger.info(
        f"Starting find_logs coroutine for query: {query} "
        f"& sort: {sort} from db: "
        f"{settings.database}"
    )
    if sort is None:
        result = await JavaLog.find(query).to_list()
    else:
        result = await JavaLog.find(query).sort(sort).to_list()
    logger.info(
        f"Found {len(result)} logs in find_logs coroutine for "
        f"query: {query} & sort: {sort} from db: "
        f"{settings.database}"
    )
    logger.info(
        f"Ending find_logs coroutine for query: {query} & sort: {sort} "
        f"from db: {settings.database}"
    )
    return result
