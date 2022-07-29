"""
Module Name: db.py
Created: 2022-07-24
Creator: JL
Change Log: 2022-07-26 - added environment settings
Summary: db handles the initialization of the database and all db operations
Functions: init, saveLogs
"""
import logging

import motor
from beanie import init_beanie
from pymongo.errors import ServerSelectionTimeoutError

from aggregator.config import get_settings
from aggregator.model import JavaLog

logger = logging.getLogger(__name__)

settings = get_settings()
client = motor.motor_asyncio.AsyncIOMotorClient(settings.connection)


async def init():
    logger.info(f"Initializing beanie with {settings.database} using {client}")
    try:
        await init_beanie(database=client[settings.database],
                          document_models=[JavaLog])
    except ServerSelectionTimeoutError as err:
        logger.fatal(f"ServerSelectionTimeoutError: {err}")
        exit()


async def save_logs(logs):
    await JavaLog.insert_many(logs)
    num_logs = len(logs)
    logger.info(f"Inserted {num_logs} into {settings.database}")
    for log in logs:
        logger.debug(f"Inserted {log}")
