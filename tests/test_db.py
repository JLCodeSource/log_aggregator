from datetime import datetime
import beanie
import string
import random
import motor
import aggregator.db
import asyncio
import pytest

from aggregator.model import JavaLog


class MockBeanie:
    # MockBeanie for beanie

    @staticmethod
    async def init_beanie(database, document_models):
        return await beanie.init_beanie(database, document_models)

# And a mock init_beanie to target the test database
# def mock_init_beanie(*args, **kwargs):
#    return MockBeanie.init_beanie(args, kwargs)

#monkeypatch.setattr(beanie, "init_beanie", mock_init_beanie)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_init(settings_override, motor_client, monkeypatch):
    # Given a motor client & a test-logs database async generator
    async def gen():
        return [i async for i in motor_client]

    motor_client = await gen()
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # When it tries to init the database
    ok = await aggregator.db.init(database, client)

    # And adds a log
    log = JavaLog(
        node="testnode",
        severity="INFO",
        jvm="jvm",
        datetime=datetime.now(),
        source="source",
        type="fanapiservice",
        message="This is a log"
    )
    await JavaLog.insert_one(log)
    asyncio.sleep(1)

    # Then it returns ok
    assert ok == "ok"
    # And it creates a database
    assert database in await client.list_database_names()
    client.drop_database(database)
