# import asyncio
from datetime import datetime
import beanie
from bson import ObjectId
import pytest
import logging
import aggregator.db
from pymongo.errors import ServerSelectionTimeoutError
from aggregator.model import JavaLog


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
        motor_client_gen, monkeypatch, logger):
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
    try:
        with pytest.raises(ServerSelectionTimeoutError):
            await aggregator.db.init(database, client)

        assert logger.record_tuples == [
            (module_name, logging.INFO,
             f"Initializing beanie with {database} using {client}"
             ),
            (module_name, logging.FATAL,
             "ServerSelectionTimeoutError: Server was unreachable "
             "within the timeout"),
        ]

    finally:
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_save_logs(motor_client_gen, logger):
    # Given a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # And an initialized  database
    try:
        await aggregator.db.init(database, client)
        logger.set_level = logging.DEBUG

        # And 2 logs
        log = JavaLog(
            node="testnode",
            severity="INFO",
            jvm="jvm",
            datetime=datetime.now(),
            source="source",
            type="fanapiservice",
            message="This is a log"
        )

        logs = []
        for i in range(2):
            logs.append(log)

        result = await aggregator.db.save_logs(logs)

        # Then the logger logs it
        assert logger.record_tuples[3][0] == module_name
        assert logger.record_tuples[3][1] == logging.INFO
        assert logger.record_tuples[3][2].startswith(
            "Started insert coroutine for 2 into db:"
        )
        assert logger.record_tuples[4][0] == module_name
        assert logger.record_tuples[4][1] == logging.INFO
        assert logger.record_tuples[4][2].startswith(
            "Inserted 2 into db:"
        )
        assert logger.record_tuples[5][0] == module_name
        assert logger.record_tuples[5][1] == logging.DEBUG
        assert logger.record_tuples[5][2].startswith(
            f"Inserted {log}"
        )
        assert logger.record_tuples[6][0] == module_name
        assert logger.record_tuples[6][1] == logging.DEBUG
        assert logger.record_tuples[6][2].startswith(
            f"Inserted {log}"
        )
        assert logger.record_tuples[7][0] == module_name
        assert logger.record_tuples[7][1] == logging.INFO
        assert logger.record_tuples[7][2].startswith(
            "Ending insert coroutine for 2 into db:"
        )

        # And it returns a list of the ids
        assert len(result.inserted_ids) == 2
        assert isinstance(result.inserted_ids[0], ObjectId)
        assert isinstance(result.inserted_ids[1], ObjectId)
    finally:
        # Set manual teardown
        await client.drop_database(database)
