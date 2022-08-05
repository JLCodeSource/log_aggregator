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
from pymongo.errors import ServerSelectionTimeoutError

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


async def save_logs(logs) -> str:
    num_logs = len(logs)
    logger.info(
        f"Started insert coroutine for {num_logs} into db: {settings.database}"
    )
    await asyncio.sleep(0)
    result = await JavaLog.insert_many(logs)

    logger.info(f"Inserted {num_logs} into db: {settings.database}")
    for log in logs:
        logger.debug(f"Inserted {log}")
    logger.info(
        f"Ending insert coroutine for {num_logs} into db: {settings.database}"
    )

    return result
