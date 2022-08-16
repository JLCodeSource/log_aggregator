# import logging
import io
import sys

import pytest
from beanie import PydanticObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.results import InsertManyResult

from aggregator import config, convert, db, view
from aggregator.model import JavaLog

module_name: str = "view"


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.parametrize("make_logs", ["one_line_log.log"], indirect=["make_logs"])
async def test_view_display_result_one_line_success(
    motor_conn: tuple[str, str], make_logs: str, mock_get_node: str
) -> None:
    # Given a motor_conn
    database: str
    conn: str
    database, conn = motor_conn

    # And a target log file
    tgt_log_file: str = make_logs

    # And a display header
    header: str = (
        "| ObjectId\t\t\t| Node\t| Severity\t| JVM\t| Timestamp\t| "
        "Source\t| Type\t| Message\t|\n"
    )

    # And an initialized database
    try:
        client: AsyncIOMotorClient = await db.init(database, conn)

        # And a log
        logs: list[JavaLog] = await convert.convert(tgt_log_file)

        # And it has saved the log
        ids: InsertManyResult | None = await db.insert_logs(logs, database)

        assert ids is not None
        id: PydanticObjectId = ids.inserted_ids[0]

        # And it gets the log
        result: JavaLog | None = await db.get_log(id, database)

        # And it has a StringIO to capture output
        capturedOutput: io.StringIO = io.StringIO()
        sys.stdout = capturedOutput

        # And the expected output is
        out: str = (
            f"{header}| {id}\t| node\t| INFO\t| jvm 1\t| "
            f"2022-07-11 09:12:02\t| ttl.test\t| SMB\t| Exec proxy\t|\n\n"
        )

        # When it tries to display the logs
        await view.display_result(result, database)

        # Then the logs are displayed
        sys.stdout = sys.__stdout__
        #
        assert capturedOutput.getvalue() == out
        # assert result == capturedOutput.getvalue()

    finally:
        client = AsyncIOMotorClient(conn)
        await client.drop_database(database)


@pytest.mark.parametrize(
    "make_logs", [("two_line_svc.log", "two_line_svc_out.log")], indirect=["make_logs"]
)
@pytest.mark.asyncio
@pytest.mark.unit
async def test_view_display_result_multi_line_success(
    motor_conn: tuple[str, str],
    make_logs: list[str],
    mock_get_node: str,
    logger: pytest.LogCaptureFixture,
    settings_override: config.Settings,
) -> None:
    # Given a motor_conn
    database: str
    conn: str
    database, conn = motor_conn

    # And target log files
    logs: list[str] = make_logs
    log_in: str = logs[0]
    log_out: str = logs[1]

    # And an initialized database
    try:
        client: AsyncIOMotorClient = await db.init(database, conn)

        # And a log
        converted_logs: list[JavaLog] = await convert.convert(log_in)

        # And it has saved the log
        await db.insert_logs(converted_logs)

        # And it has a query
        query: str = JavaLog.node == "node"

        # And it gets the log
        results: list[JavaLog] = await db.find_logs(query, sort=None)

        # And it has a StringIO to capture output
        capturedOutput: io.StringIO = io.StringIO()
        sys.stdout = capturedOutput

        # And out has had placeholder "objectid" values replaced
        with open(log_out, "r") as f:
            content: str = f.read()
            for i in range(len(results)):
                content = content.replace(f"objectid{i}", str(results[i].id))

        # And the expected output is
        out: str = content

        # When it tries to display the logs
        await view.display_result(results, database)

        # Then the logs are displayed
        sys.stdout = sys.__stdout__

        assert out == capturedOutput.getvalue()

        # Then the logger logs it
        """
        num_logs: int = len(results)
        assert logger.record_tuples[0] == (
            module_name, logging.INFO,
            f"Started display_results coroutine for {num_logs} logs from db: "
            f"{database}"
        )
        """

    finally:
        client = AsyncIOMotorClient(conn)
        await client.drop_database(database)
