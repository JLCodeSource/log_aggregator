"""
Module Name: db.py 
Created: 2022-07-24
Creator: JL
Change Log: Initial
Summary: db handles the initialization of the database and all db operations
Functions: init, saveLogs
"""
import logging

import motor
from beanie import init_beanie

from config import connection, database
from model import JavaLog

logger = logging.getLogger(__name__)

client = motor.motor_asyncio.AsyncIOMotorClient(connection)


async def init():
    logger.info(f"Initializing beanie with {database} using {client}")
    await init_beanie(database=client[database], document_models=[JavaLog])


async def saveLogs(logs):
    await JavaLog.insert_many(logs)
    numLogs = len(logs)
    logger.info(f"Inserted {numLogs} into {database}")
    for log in logs:
        logger.debug(f"Inserted {log}")
