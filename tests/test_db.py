# import asyncio
# from datetime import datetime
import beanie
import pytest
import logging
import aggregator.db
from pymongo.errors import ServerSelectionTimeoutError
# from aggregator.model import JavaLog


module_name = "aggregator.db"


class MockBeanie:
    # MockBeanie for beanie for server_timeout

    @staticmethod
    def init_beanie_server_timeout():
        raise ServerSelectionTimeoutError


@pytest.mark.asyncio
@pytest.mark.unit
async def test_init(motor_client_gen, logger, add_one):
    # Given a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    try:
        # When it tries to init the database
        ok = await aggregator.db.init(database, client)

        # And it adds a log
        await add_one

        # Then it returns ok
        assert ok == "ok"
        # And it creates a database
        assert database in await client.list_database_names()
        # And the logger logs it
        assert logger.record_tuples == [
            (module_name, logging.INFO,
                f"Initializing beanie with {database} using {client}"
             ),
            (module_name, logging.INFO,
                f"Initialized beanie with {database} using {client}"
             ),
            (module_name, logging.INFO,
                f"Completed initialization of beanie with {database}"
                f" using {client}"
             ),
        ]

    finally:
        # Set Manual Teardown

        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_init_server_timeout(
        motor_client_gen, monkeypatch, logger, add_one):
    # Given a mock init_beanie_server_timeout to target the test database
    def mock_init_beanie_server_timeout(*args, **kwargs):
        return MockBeanie.init_beanie_server_timeout()

    monkeypatch.setattr(beanie, "init_beanie",
                        mock_init_beanie_server_timeout)

    # And a motor_client_generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # When it tries to init the database
    # It raises a ServerSelectionTimeoutError
    with pytest.raises(ServerSelectionTimeoutError):
        await aggregator.db.init(database, client)
