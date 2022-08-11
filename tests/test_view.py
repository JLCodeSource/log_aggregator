import logging
import io
import pytest
import sys
from aggregator import convert, db, view
from aggregator.model import JavaLog

module_name = "view"


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.parametrize(
    "make_logs", ["one_line_log.log"], indirect=["make_logs"])
async def test_view_display_result_one_line_success(
    motor_conn, make_logs, mock_get_node
):
    # Given a motor_conn
    database, conn = await motor_conn

    # And a target log file
    tgt_log_file = make_logs

    # And a display header
    header = (
        "| ObjectId\t\t\t| Node\t| Severity\t| JVM\t| Timestamp\t| "
        "Source\t| Type\t| Message\t|\n"
    )

    # And an initialized database
    try:
        client = await db.init(database, conn)

        # And a log
        logs = await convert.convert(tgt_log_file)

        # And it has saved the log
        ids = await db.insert_logs(logs, database)
        id = ids.inserted_ids[0]

        # And it gets the log
        result = await db.get_log(id, database)

        # And it has a StringIO to capture output
        capturedOutput = io.StringIO()
        sys.stdout = capturedOutput

        # And the expected output is
        out = (
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
        await client.drop_database(database)


@pytest.mark.parametrize(
    "make_logs", [("two_line_svc.log", "two_line_svc_out.log")],
    indirect=["make_logs"])
@pytest.mark.asyncio
@pytest.mark.unit
async def test_view_display_result_multi_line_success(
    motor_conn, make_logs, mock_get_node, logger,
    settings_override
):
    # Given a motor_conn
    database, conn = await motor_conn

    # And target log files
    logs = make_logs
    log_in = logs[0]
    log_out = logs[1]

    # And an initialized database
    try:
        client = await db.init(database, conn)

        # And a log
        logs = await convert.convert(log_in)

        # And it has saved the log
        await db.insert_logs(logs)

        # And it has a query
        query = (JavaLog.node == "node")

        # And it gets the log
        results = await db.find_logs(query)

        # And it has a StringIO to capture output
        capturedOutput = io.StringIO()
        sys.stdout = capturedOutput

        # And out has had placeholder "objectid" values replaced
        with open(log_out, "r") as f:
            content = f.read()
            for i in range(len(results)):
                content = content.replace(f"objectid{i}", str(results[i].id))

        # And the expected output is
        out = content

        # When it tries to display the logs
        await view.display_result(results, database)

        # Then the logger logs it
        num_logs = len(results)
        logger.record_tuples[0] == (
            module_name, logging.INFO,
            f"Started display_results coroutine for {num_logs} logs from db: "
            f"{database}"
        )

        # Then the logs are displayed
        sys.stdout = sys.__stdout__

        assert out == capturedOutput.getvalue()

        # And the logger logs it

    finally:
        await client.drop_database(database)
