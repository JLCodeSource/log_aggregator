import logging
from datetime import datetime
from typing import Any, Coroutine, Literal, NoReturn

import beanie
import motor.motor_asyncio
import pytest
from beanie import PydanticObjectId
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import ValidationError
from pymongo.errors import InvalidOperation, ServerSelectionTimeoutError
from pymongo.results import InsertManyResult
from pytest_mock_resources.fixture.database.mongo import create_mongo_fixture

from aggregator import convert, db
from aggregator.model import JavaLog

module_name: Literal["aggregator.db"] = "aggregator.db"
wrong_id: PydanticObjectId = PydanticObjectId("608da169eb9e17281f0ab2ff")
mongo = create_mongo_fixture()


class MockBeanie:
    # MockBeanie for beanie for server_timeout

    @staticmethod
    def beanie_server_timeout() -> NoReturn:
        raise ServerSelectionTimeoutError


class MockJavaLog:
    # MockJavaLog is used for testing the database

    @staticmethod
    def insert_many_invalid(*args, **kwargs) -> NoReturn:
        raise InvalidOperation

    @staticmethod
    def insert_many_server_timeout(*args, **kwargs) -> NoReturn:
        raise ServerSelectionTimeoutError


@pytest.mark.asyncio
@pytest.mark.unit
async def test_init(
    motor_conn: tuple[str, str],
    logger: pytest.LogCaptureFixture,
    add_one: Coroutine[Any, Any, JavaLog | None],
) -> None:
    # Given a motor_conn & database
    database: str
    conn: str
    database, conn = motor_conn

    try:
        # When it tries to init the database
        client: AsyncIOMotorClient = await db.init(database, conn)

        # And it adds a log
        await add_one

        # Then it creates a database
        assert database in await client.list_database_names()
        # And the logger logs it
        assert logger.record_tuples == [
            (
                module_name,
                logging.INFO,
                f"Initializing beanie with {database} using {conn}",
            ),
            (
                module_name,
                logging.INFO,
                f"Initialized beanie with {database} using {conn}",
            ),
            (
                module_name,
                logging.INFO,
                f"Completed initialization of beanie with {database}" f" using {conn}",
            ),
        ]

    finally:
        # Set Manual Teardown
        client = await db.init(database, conn)
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_init_server_timeout(
    motor_conn: tuple[str, str],
    monkeypatch: pytest.MonkeyPatch,
    logger: pytest.LogCaptureFixture,
) -> None:
    # Given a mock init_beanie_server_timeout to target the test database
    def mock_beanie_server_timeout(*args, **kwargs) -> NoReturn:
        raise MockBeanie.beanie_server_timeout()

    monkeypatch.setattr(beanie, "init_beanie", mock_beanie_server_timeout)

    # And a motor_conn & database
    database: str
    conn: str
    database, conn = motor_conn

    # When it tries to init the database
    # It raises a ServerSelectionTimeoutError
    try:
        with pytest.raises(ServerSelectionTimeoutError):
            await db.init(database, conn)

        assert logger.record_tuples == [
            (
                module_name,
                logging.INFO,
                f"Initializing beanie with {database} using {conn}",
            ),
            (
                module_name,
                logging.FATAL,
                "ServerSelectionTimeoutError: Server was unreachable "
                "within the timeout",
            ),
        ]

    finally:
        # Set manual teardown
        client = AsyncIOMotorClient(conn)
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_insert_logs_success(
    motor_conn: tuple[str, str], logger: pytest.LogCaptureFixture
) -> None:
    # Given a motor_conn & database
    database: str
    conn: str
    database, conn = motor_conn

    # And an initialized  database
    try:
        client: AsyncIOMotorClient = await db.init(database, conn)

        # And 2 logs
        log: JavaLog = JavaLog(
            node="testnode",
            severity="INFO",
            jvm="jvm",
            datetime=datetime.now(),
            source="source",
            type="fanapiservice",
            message="This is a log",
        )

        logs: list[JavaLog] = []
        for _ in range(2):
            logs.append(log)

        result: InsertManyResult | None = await db.insert_logs(logs)

        # Then the logger logs it
        # Get lists of types
        mods: list[str] = []
        lvls: list[int] = []
        msgs: list[str] = []

        mods, lvls, msgs = pytest.helpers.log_recorder(  # type: ignore
            logger.record_tuples
        )  # type: ignore

        count_logs: int = pytest.helpers.count_items(  # type: ignore
            msgs, f"Inserted {log}"
        )

        count_debugs: int = pytest.helpers.count_items(  # type: ignore
            lvls, logging.DEBUG
        )

        count_infos: int = pytest.helpers.count_items(  # type: ignore
            lvls, logging.INFO
        )

        # The logger logs modules
        assert all(module == module_name for module in mods)

        # And logger logs logs
        assert count_logs == 2

        # And logger logs levels
        assert count_debugs == 2
        assert count_infos == 6

        # And logger includes expected values
        assert any((s.startswith("Inserted 2 logs into db:") for s in msgs))
        assert any(
            (
                s.startswith("Started insert_logs coroutine for 2 logs " "into db:")
                for s in msgs
            )
        )
        assert any(
            (
                s.startswith("Ending insert_logs coroutine for 2 logs " "into db:")
                for s in msgs
            )
        )

        # And it returns a list of the ids
        assert result is not None
        assert len(result.inserted_ids) == 2
        assert isinstance(result.inserted_ids[0], ObjectId)
        assert isinstance(result.inserted_ids[1], ObjectId)
    finally:
        client = await db.init(database, conn)
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_insert_logs_servertimeout(
    motor_conn: tuple[str, str],
    logger: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a mock javalog_server_timeout to target the test database
    def mock_insert_logs_server_timeout(*args, **kwargs) -> NoReturn:
        raise MockJavaLog.insert_many_server_timeout()

    monkeypatch.setattr(JavaLog, "insert_many", mock_insert_logs_server_timeout)

    # And a motor_cobnn & database
    database: str
    conn: str
    database, conn = motor_conn

    # And an initialized  database
    try:
        client: AsyncIOMotorClient = await db.init(database, conn)

        # When it tries to save the logs
        # Then it raises a ServerSelectionTimeoutError
        with pytest.raises(ServerSelectionTimeoutError):
            await db.insert_logs([wrong_id], database)

        # And the logger logs the error
        assert logger.record_tuples[-2] == (
            module_name,
            logging.ERROR,
            f"ErrorType: <class 'pymongo.errors.ServerSelectionTimeoutError'>"
            f" - coroutine insert_logs for "
            f"1 logs failed for db: {database}",
        )

    finally:
        client = await db.init(database, conn)
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_insert_logs_invalid_operation_error(
    motor_conn: tuple[str, str],
    logger: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a MockJavaLog
    def mock_javalog_raises_invalid_operation(*args, **kwargs) -> NoReturn:
        raise MockJavaLog.insert_many_invalid(*args, **kwargs)

    monkeypatch.setattr(JavaLog, "insert_many", mock_javalog_raises_invalid_operation)

    # And a motor_conn & database
    database: str
    conn: str
    database, conn = motor_conn

    # And an initialized database
    try:
        client: AsyncIOMotorClient = await db.init(database, conn)

        # And 2 logs
        log: JavaLog = JavaLog(
            node="testnode",
            severity="INFO",
            jvm="jvm",
            datetime=datetime.now(),
            source="source",
            type="fanapiservice",
            message="This is a log",
        )

        logs: list[JavaLog] = []
        for _ in range(2):
            logs.append(log)

        # When it tries to save the logs
        # Then it raises a InvalidOperation
        with pytest.raises(InvalidOperation):
            await db.insert_logs(logs, database)

        # And the logger logs the error
        assert logger.record_tuples[-2] == (
            module_name,
            logging.ERROR,
            "ErrorType: <class 'pymongo.errors.InvalidOperation'> "
            f"- coroutine insert_logs for {len(logs)} logs failed for db: "
            f"{database}",
        )
        assert logger.record_tuples[-1] == (
            module_name,
            logging.INFO,
            f"Ending insert_logs coroutine for {len(logs)} logs into db: "
            f"{database}",
        )

    finally:
        client = await db.init(database, conn)
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_insert_logs_none(
    motor_conn: tuple[str, str], logger: pytest.LogCaptureFixture
) -> None:
    # Given a motor_conn & database
    database: str
    conn: str
    database, conn = motor_conn
    # And an initialized database
    try:
        client: AsyncIOMotorClient = await db.init(database, conn)

        # When it tries to insert None logs
        # Then it returns None
        result: InsertManyResult | None = await db.insert_logs(database=database)

        assert result is None

        # And the logger logs the error
        assert logger.record_tuples[-2] == (
            module_name,
            logging.WARNING,
            "Started insert_logs coroutine for None logs into db: " f"{database}",
        )
        assert logger.record_tuples[-1] == (
            module_name,
            logging.WARNING,
            f"Ending insert_logs coroutine for None logs into db: " f"{database}",
        )

    finally:
        client = await db.init(database, conn)
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_get_log_successfully(
    motor_conn: tuple[str, str],
    get_datetime: datetime,
    logger: pytest.LogCaptureFixture,
) -> None:
    # Given a motor_conn & database
    database: str
    conn: str
    database, conn = motor_conn

    # And an initialized database
    try:
        client: AsyncIOMotorClient = await db.init(database, conn)

        # And 2 logs
        log: JavaLog = JavaLog(
            node="testnode",
            severity="INFO",
            jvm="jvm",
            datetime=get_datetime,
            source="source",
            type="fanapiservice",
            message="This is a log",
        )

        logs: list[JavaLog] = []
        for _ in range(2):
            logs.append(log)

        # And it has saved the logs
        result: InsertManyResult | None = await db.insert_logs(logs, database)

        assert result is not None
        # When it tries to get the log
        returned_log: JavaLog | None = await db.get_log(
            result.inserted_ids[0], database
        )

        # Then the returned_log matches the log
        assert returned_log is not None
        assert returned_log.node == log.node
        assert returned_log.severity == log.severity
        assert returned_log.jvm == log.jvm
        assert get_datetime == log.datetime
        assert returned_log.source == log.source
        assert returned_log.type == log.type
        assert returned_log.message == log.message

        # And the logger logs it
        assert logger.record_tuples[-3] == (
            module_name,
            logging.INFO,
            f"Starting get_log coroutine for {result.inserted_ids[0]} "
            f"from db: {database}",
        )
        assert logger.record_tuples[-2] == (
            module_name,
            logging.INFO,
            f"Got {result.inserted_ids[0]} from db: {database}",
        )
        assert logger.record_tuples[-1] == (
            module_name,
            logging.INFO,
            f"Ending get_log coroutine for {result.inserted_ids[0]} from db: "
            f"{database}",
        )

    finally:
        client = await db.init(database, conn)
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_get_log_none(
    motor_conn: tuple[str, str], logger: pytest.LogCaptureFixture
) -> None:
    # Given a motor_conn & database
    database: str
    conn: str
    database, conn = motor_conn

    # And an initialized database
    try:
        client: AsyncIOMotorClient = await db.init(database, conn)

        # When it tries to get the logs with a NoneType
        # Then it raises an error
        with pytest.raises(ValidationError):
            await db.get_log(None, database=database)

        # And the logger logs it
        assert logger.record_tuples[-2] == (
            module_name,
            logging.ERROR,
            f"Error: <class 'pydantic.error_wrappers.ValidationError'> "
            f"- get_log coroutine for None failed for db: "
            f"{database}",
        )
        assert logger.record_tuples[-1] == (
            module_name,
            logging.INFO,
            f"Ending get_log coroutine for None from db: " f"{database}",
        )

    finally:
        client = await db.init(database, conn)
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_get_log_wrong_id(
    motor_conn: tuple[str, str], logger: pytest.LogCaptureFixture
) -> None:
    # Given a motor_conn & database
    database: str
    conn: str
    database, conn = motor_conn
    # And a missing log_id
    log_id: PydanticObjectId = wrong_id

    # And an initialized database
    try:
        client: AsyncIOMotorClient = await db.init(database, conn)

        # When it tries to get the logs with a missing log
        returned_log: JavaLog | None = await db.get_log(
            log_id, database
        )  # type: ignore

        # Then the logger logs it
        assert logger.record_tuples[-2][0] == module_name
        assert logger.record_tuples[-2][1] == logging.INFO
        assert logger.record_tuples[-2][2].startswith(
            f"When getting {log_id} from db {database} found " f"{returned_log}"
        )

    finally:
        client = await db.init(database, conn)
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_get_log_server_timeout(
    motor_conn: tuple[str, str],
    logger: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a mock javalog_server_timeout to target the test database
    def mock_javalog_server_timeout(*args, **kwargs) -> NoReturn:
        raise MockJavaLog.insert_many_server_timeout()

    monkeypatch.setattr(JavaLog, "get", mock_javalog_server_timeout)
    # And a motor_conn & database
    database: str
    conn: str
    database, conn = motor_conn

    # And an id
    log_id: PydanticObjectId = wrong_id

    # And an initialized database
    try:
        client: AsyncIOMotorClient = await db.init(database, conn)

        # When it tries to get the logs with a missing log
        with pytest.raises(ServerSelectionTimeoutError):
            await db.get_log(log_id, database)  # type: ignore

        # And the logger logs the error
        assert logger.record_tuples[-2] == (
            module_name,
            logging.ERROR,
            f"Error: <class 'pymongo.errors.ServerSelectionTimeoutError'> "
            f"- get_log coroutine for {wrong_id} "
            f"failed for db: {database}",
        )

    finally:
        client = await db.init(database, conn)
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_find_logs_successfully(
    motor_conn: tuple[str, str], get_datetime: datetime, logger: logging.Logger
) -> None:
    # Given a motor_conn & database
    database: str
    conn: str
    database, conn = motor_conn

    # And an initialized database
    try:
        client: AsyncIOMotorClient = await db.init(database, conn)

        # And 2 logs
        log: JavaLog = JavaLog(
            node="testnode",
            severity="INFO",
            jvm="jvm",
            datetime=get_datetime,
            source="source",
            type="fanapiservice",
            message="This is a log",
        )

        logs: list[JavaLog] = []
        for _ in range(2):
            logs.append(log)

        # And it has saved the logs
        await db.insert_logs(logs, database)

        # And it has a query
        query: str = JavaLog.node == "testnode"

        # When it tries to find the logs
        result: list[JavaLog] = await db.find_logs(query, sort=None, database=database)

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
        mods: list[str]
        lvls: list[int]
        msgs: list[str]
        mods, lvls, msgs = pytest.helpers.log_recorder(  # type: ignore
            logger.record_tuples  # type: ignore
        )

        count_infos: int = 0
        for level in lvls:
            if level == logging.INFO:
                count_infos = count_infos + 1

        # The logger logs modules
        assert all(module == module_name for module in mods)

        assert count_infos == 9

        # And logger includes expected values
        assert any(
            (
                s == f"Starting find_logs coroutine for "
                f"query: {query} & sort: None "
                f"from db: {database}"
                for s in msgs
            )
        )
        assert any(
            (
                s == f"Found 2 logs in find_logs coroutine for "
                f"query: {query} & sort: None "
                f"from db: {database}"
            )
            for s in msgs
        )
        assert any(
            (
                s == f"Ending find_logs coroutine for query: {query} "
                f"& sort: None from db: {database}"
            )
            for s in msgs
        )

    finally:
        client = await db.init(database, conn)
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.parametrize("make_logs", ["simple_svc.log"], indirect=["make_logs"])
async def test_find_logs_with_sort(
    motor_conn: tuple[str, str], make_logs: str, mock_get_node: str
) -> None:
    # Given a motor_client, database & db_log_name
    database: str
    conn: str
    database, conn = motor_conn

    # And a target log file
    tgt_log_file: str = make_logs

    # And an initialized database
    try:
        await db.init(database, conn)

        # And some logs
        logs: list[JavaLog] = await convert.convert(tgt_log_file)

        # And it has saved the logs
        await db.insert_logs(logs)

        # And it has a query
        query: str = JavaLog.node == "node"

        # And it has a sort
        sort: str = "-datetime"

        # When it tries to find the logs
        result: list[JavaLog] = await db.find_logs(query, sort)

        # Then it returns both logs
        assert len(result) == 5
        assert all(isinstance(r, JavaLog) for r in result)
        timestamps: tuple[datetime, datetime, datetime, datetime, datetime] = (
            datetime(2022, 7, 11, 9, 15, 51),
            datetime(2022, 7, 11, 9, 14, 51),
            datetime(2022, 7, 11, 9, 13, 1),
            datetime(2022, 7, 11, 9, 12, 55),
            datetime(2022, 7, 11, 9, 12, 2),
        )
        for i in range(len(result)):
            assert result[i].datetime == timestamps[i]

    finally:
        # Set manual teardown
        client = motor.motor_asyncio.AsyncIOMotorClient(conn)
        await client.drop_database(database)


"""
@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.mock
async def test_db_pmr(
        settings_override_pmr: tuple[Settings, dict[str, Any]],
        add_one: Coroutine[Any, Any, JavaLog | None]) -> None:
    # Given a pmr_database & connection
    settings: Settings
    pmr_creds: dict[str, Any]
    settings, pmr_creds = settings_override_pmr
    check: dict[str, Any] = pmr_creds

    database: str = settings.database
    conn: str = settings.connection

    try:
        # When it tries to init the database
        client: AsyncIOMotorClient = await db.init(database, conn)

        # And it adds a log
        await add_one

        # Then it creates a database
        assert database in await client.list_database_names()

    finally:
        # Set Manual Teardown
        client: AsyncIOMotorClient = await db.init(database, conn)
        await client.drop_database(database)
"""
