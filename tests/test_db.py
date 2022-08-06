from datetime import datetime
from pydantic import ValidationError
import beanie
from bson import ObjectId
import pytest
import logging
import aggregator.db
from pymongo.errors import ServerSelectionTimeoutError, InvalidOperation
from aggregator.model import JavaLog


module_name = "aggregator.db"
wrong_id = "608da169eb9e17281f0ab2ff"


class MockBeanie:
    # MockBeanie for beanie for server_timeout

    @staticmethod
    def beanie_server_timeout():
        raise ServerSelectionTimeoutError


class MockJavaLog:
    # MockJavaLog is used for testing the database

    @staticmethod
    def insert_many_invalid(*args, **kwargs):
        raise InvalidOperation

    @staticmethod
    def insert_many_server_timeout(*args, **kwargs):
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
    def mock_beanie_server_timeout(*args, **kwargs):
        return MockBeanie.beanie_server_timeout()

    monkeypatch.setattr(beanie, "init_beanie",
                        mock_beanie_server_timeout)

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
async def test_insert_logs_success(motor_client_gen, logger):
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

        result = await aggregator.db.insert_logs(logs)

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
        assert any((s.startswith("Inserted 2 logs into db:")
                    for s in messages))
        assert any((s.startswith("Started insert_logs coroutine for 2 logs "
                                 "into db:")
                    for s in messages))
        assert any((s.startswith("Ending insert_logs coroutine for 2 logs "
                                 "into db:")
                    for s in messages))

        # And it returns a list of the ids
        assert len(result.inserted_ids) == 2
        assert isinstance(result.inserted_ids[0], ObjectId)
        assert isinstance(result.inserted_ids[1], ObjectId)
    finally:
        # Set manual teardown
        await client.drop_database(database)


@ pytest.mark.asyncio
@ pytest.mark.unit
async def test_insert_logs_servertimeout(
        motor_client_gen, logger, monkeypatch, settings_override):
    # Given a mock javalog_server_timeout to target the test database
    def mock_insert_logs_server_timeout(*args, **kwargs):
        return MockJavaLog.insert_many_server_timeout()

    monkeypatch.setattr(JavaLog, "insert_many",
                        mock_insert_logs_server_timeout)
    # And a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # And a mocked database name output for logs
    database_log_name = settings_override.database

    # And an initialized  database
    try:
        await aggregator.db.init(database, client)

        # When it tries to save the logs
        # Then it raises a ServerSelectionTimeoutError
        with pytest.raises(ServerSelectionTimeoutError):
            await aggregator.db.insert_logs([wrong_id])

        # And the logger logs the error
        assert logger.record_tuples[-2] == (
            module_name, logging.ERROR,
            f"ErrorType: <class 'pymongo.errors.ServerSelectionTimeoutError'>"
            f" - coroutine insert_logs for "
            f"1 logs failed for db: {database_log_name}"
        )

    finally:
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_insert_logs_invalid_operation_error(
        motor_client_gen, logger, monkeypatch, settings_override):
    # Given a MockJavaLog
    def mock_javalog_raises_invalid_operation(*args, **kwargs):
        return MockJavaLog.insert_many_invalid(*args, **kwargs)

    monkeypatch.setattr(JavaLog, "insert_many",
                        mock_javalog_raises_invalid_operation)

    # And a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # And a mocked database name output for logs
    database_log_name = settings_override.database

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
            await aggregator.db.insert_logs(logs)

        # And the logger logs the error
        assert logger.record_tuples[-2] == (
            module_name, logging.ERROR,
            "ErrorType: <class 'pymongo.errors.InvalidOperation'> "
            f"- coroutine insert_logs for {len(logs)} logs failed for db: "
            f"{database_log_name}"
        )
        assert logger.record_tuples[-1] == (
            module_name, logging.INFO,
            f"Ending insert_logs coroutine for {len(logs)} logs into db: "
            f"{database_log_name}"
        )

    finally:
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_get_log_successfully(
        motor_client_gen, get_datetime, logger,
        settings_override):
    # Given a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # And a mocked database name output for logs
    database_log_name = settings_override.database

    # And an initialized database
    try:
        await aggregator.db.init(database, client)

        # And 2 logs
        log = JavaLog(
            node="testnode",
            severity="INFO",
            jvm="jvm",
            datetime=get_datetime,
            source="source",
            type="fanapiservice",
            message="This is a log"
        )

        logs = []
        for _ in range(2):
            logs.append(log)

        # And it has saved the logs
        result = await aggregator.db.insert_logs(logs)

        # When it tries to get the log
        returned_log = await aggregator.db.get_log(result.inserted_ids[0])

        # Then the returned_log matches the log
        assert returned_log.node == log.node
        assert returned_log.severity == log.severity
        assert returned_log.jvm == log.jvm
        assert get_datetime == log.datetime
        assert returned_log.source == log.source
        assert returned_log.type == log.type
        assert returned_log.message == log.message

        # And the logger logs it
        assert logger.record_tuples[-3] == (
            module_name, logging.INFO,
            f"Starting get_log coroutine for {result.inserted_ids[0]} "
            f"from db: {database_log_name}"
        )
        assert logger.record_tuples[-2] == (
            module_name, logging.INFO,
            f"Got {result.inserted_ids[0]} from db: {database_log_name}"
        )
        assert logger.record_tuples[-1] == (
            module_name, logging.INFO,
            f"Ending get_log coroutine for {result.inserted_ids[0]} from db: "
            f"{database_log_name}"
        )

    finally:
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_get_log_none(
        motor_client_gen,
        logger,
        settings_override):
    # Given a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # And a mocked database name output for logs
    database_log_name = settings_override.database

    # And an initialized database
    try:
        await aggregator.db.init(database, client)

        # When it tries to get the logs with a NoneType
        # Then it raises an error
        with pytest.raises(ValidationError):
            await aggregator.db.get_log()

        # And the logger logs it
        assert logger.record_tuples[-2] == (
            module_name, logging.ERROR,
            f"Error: <class 'pydantic.error_wrappers.ValidationError'> "
            f"- get_log coroutine for None failed for db: "
            f"{database_log_name}"
        )
        assert logger.record_tuples[-1] == (
            module_name, logging.INFO,
            f"Ending get_log coroutine for None from db: "
            f"{database_log_name}"
        )

    finally:
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_get_log_wrong_id(
        motor_client_gen,
        logger,
        settings_override):
    # Given a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # And a missing log_id
    log_id = wrong_id

    # And a mocked database name output for logs
    database_log_name = settings_override.database

    # And an initialized database
    try:
        await aggregator.db.init(database, client)

        # When it tries to get the logs with a missing log
        returned_log = await aggregator.db.get_log("608da169eb9e17281f0ab2ff")

        # Then the logger logs it
        assert logger.record_tuples[-2][0] == module_name
        assert logger.record_tuples[-2][1] == logging.INFO
        assert logger.record_tuples[-2][2].startswith(
            f"When getting {log_id} from db {database_log_name} found "
            f"{returned_log}"
        )

    finally:
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_get_log_server_timeout(
        motor_client_gen,
        logger,
        settings_override,
        monkeypatch):
    # Given a mock javalog_server_timeout to target the test database
    def mock_javalog_server_timeout(*args, **kwargs):
        return MockJavaLog.insert_many_server_timeout()

    monkeypatch.setattr(JavaLog, "get",
                        mock_javalog_server_timeout)
    # And a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # And a mocked database name output for logs
    database_log_name = settings_override.database

    # And an initialized database
    try:
        await aggregator.db.init(database, client)

        # When it tries to get the logs with a missing log
        with pytest.raises(ServerSelectionTimeoutError):
            await aggregator.db.get_log(wrong_id)

        # And the logger logs the error
        assert logger.record_tuples[-2] == (
            module_name, logging.ERROR,
            f"Error: <class 'pymongo.errors.ServerSelectionTimeoutError'> "
            f"- get_log coroutine for {wrong_id} "
            f"failed for db: {database_log_name}"
        )

    finally:
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_find_logs_successfully(
    motor_client_gen, get_datetime, logger,
    settings_override
):
    # Given a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # And a mocked database name output for logs
    database_log_name = settings_override.database

    # And an initialized database
    try:
        await aggregator.db.init(database, client)

        # And 2 logs
        log = JavaLog(
            node="testnode",
            severity="INFO",
            jvm="jvm",
            datetime=get_datetime,
            source="source",
            type="fanapiservice",
            message="This is a log"
        )

        logs = []
        for _ in range(2):
            logs.append(log)

        # And it has saved the logs
        await aggregator.db.insert_logs(logs)

        # And it has a query
        query = (JavaLog.node == "testnode")

        # When it tries to find the logs
        result = await aggregator.db.find_logs(query)

        # Then it returns both logs
        assert len(result) == 2
        assert isinstance(result[0], JavaLog)
        assert isinstance(result[1], JavaLog)

        # And the returned logs match the logs
        for i in range(len(result)):
            assert result[i].node == log.node
            assert result[i].severity == log.severity
            assert result[i].jvm == log.jvm
            assert result[i].datetime == get_datetime
            assert result[i].source == log.source
            assert result[i].type == log.type
            assert result[i].message == log.message

        # And the logger logs it
        # Get lists of types
        modules = []
        levels = []
        messages = []

        for recorded_log in logger.record_tuples:
            modules.append(recorded_log[0])
            levels.append(recorded_log[1])
            messages.append(recorded_log[2])

        count_infos = 0
        for level in levels:
            if level == logging.INFO:
                count_infos = count_infos + 1

        # The logger logs modules
        assert all(module == module_name for module in modules)

        assert count_infos == 9

        # And logger includes expected values
        assert any((s == f"Starting find_logs coroutine for {query} "
                    f"from db: {database_log_name}" for s in messages))
        assert any((s == f"Found 2 logs in find_logs coroutine for "
                    f"{query} from db: {database_log_name}")
                   for s in messages)
        assert any((s == f"Ending find_logs coroutine for {query} "
                    f"from db: {database_log_name}")
                   for s in messages)

    finally:
        # Set manual teardown
        await client.drop_database(database)
