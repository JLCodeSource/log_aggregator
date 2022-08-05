# import asyncio
from datetime import datetime
import beanie
from bson import ObjectId
import pytest
import logging
import aggregator.db
from pymongo.errors import ServerSelectionTimeoutError, InvalidOperation
from aggregator.model import JavaLog


module_name = "aggregator.db"


class MockBeanie:
    # MockBeanie for beanie for server_timeout

    @staticmethod
    def init_beanie_server_timeout():
        raise ServerSelectionTimeoutError


class MockJavaLog:
    # MockJavaLog is used for testing the database

    @staticmethod
    def insert_many(*args, **kwargs):
        raise InvalidOperation


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
        for _ in range(2):
            logs.append(log)

        result = await aggregator.db.save_logs(logs)

        # Then the logger logs it
        # Get lists of types
        modules = []
        levels = []
        messages = []

        for recorded_log in logger.record_tuples:
            modules.append(recorded_log[0])
            levels.append(recorded_log[1])
            messages.append(recorded_log[2])

        count_logs = 0
        for message in messages:
            if message == f"Inserted {log}":
                count_logs = count_logs + 1

        count_debugs = 0
        for level in levels:
            if level == logging.DEBUG:
                count_debugs = count_debugs + 1

        count_infos = 0
        for level in levels:
            if level == logging.INFO:
                count_infos = count_infos + 1

        # The logger logs modules
        assert all(module == module_name for module in modules)

        # And logger logs logs
        assert count_logs == 2

        # And logger logs levels
        assert count_debugs == 2
        assert count_infos == 6

        # And logger includes expected values
        assert (s.startswith("Inserted 2 into db:") for s in messages)
        assert (s.startswith("Started insert coroutine for 2 into db:")
                for s in messages)
        assert (s.startswith("Ending insert coroutine for 2 into db:")
                for s in messages)

        # And it returns a list of the ids
        assert len(result.inserted_ids) == 2
        assert isinstance(result.inserted_ids[0], ObjectId)
        assert isinstance(result.inserted_ids[1], ObjectId)
    finally:
        # Set manual teardown
        await client.drop_database(database)


@ pytest.mark.asyncio
@ pytest.mark.unit
async def test_save_logs_servertimeout(motor_client_gen, logger, monkeypatch):
    # Given a mock init_beanie_server_timeout to target the test database
    def mock_init_beanie_server_timeout(*args, **kwargs):
        return MockBeanie.init_beanie_server_timeout()

    monkeypatch.setattr(beanie, "init_beanie",
                        mock_init_beanie_server_timeout)
    # And a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # When it tries to initialize the database
    # Then it raises a ServerSelectionTimeoutError
    try:
        with pytest.raises(ServerSelectionTimeoutError):
            await aggregator.db.init(database, client)

        # And the logger logs the error
        assert logger.record_tuples[1] == (
            module_name, logging.FATAL,
            "ServerSelectionTimeoutError: Server was unreachable "
            "within the timeout"
        )

    finally:
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_save_logs_invalid_operation_error(
        motor_client_gen, logger, monkeypatch):
    # Given a MockJavaLog
    def mock_javalog_raises_invalid_operation(*args, **kwargs):
        return MockJavaLog.insert_many(*args, **kwargs)

    monkeypatch.setattr(JavaLog, "insert_many",
                        mock_javalog_raises_invalid_operation)

    # And a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # And an initialized database
    try:
        await aggregator.db.init(database, client)

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

    # When it tries to save the logs
    # Then it raises a InvalidOperation
        with pytest.raises(InvalidOperation):
            await aggregator.db.save_logs(logs)

        # And the logger logs the error
        assert logger.record_tuples[-1] == (
            module_name, logging.ERROR,
            "Error InvalidOperation"
        )

    finally:
        # Set manual teardown
        await client.drop_database(database)
