from beanie import Document, Indexed, init_beanie
import motor
from model import JavaLog

connection = "mongodb://root:example@localhost:27017/?authMechanism=DEFAULT"
database = "logs"
client = motor.motor_asyncio.AsyncIOMotorClient(connection)


async def init():
    await init_beanie(database=client[database], document_models=[JavaLog])
