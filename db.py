from beanie import init_beanie
import motor
from model import JavaLog
from vars import database, connection

client = motor.motor_asyncio.AsyncIOMotorClient(connection)


async def init():
    await init_beanie(database=client[database], document_models=[JavaLog])


async def saveLogs(logs):
    await JavaLog.insert_many(logs)
