"""db.py handles the initialization of the database and all database operations
"""
from beanie import init_beanie
import motor
from model import JavaLog
from config import database, connection

client = motor.motor_asyncio.AsyncIOMotorClient(connection)


async def init():
    await init_beanie(database=client[database], document_models=[JavaLog])


async def saveLogs(logs):
    await JavaLog.insert_many(logs)
