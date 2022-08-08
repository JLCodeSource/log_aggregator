import logging
import io
import pytest
import sys
from aggregator import convert, db, view
from aggregator.model import JavaLog

module_name = "view"


@pytest.mark.asyncio
@pytest.mark.unit
async def test_view_display_result_success(
    motor_client, temp_one_line_log, mock_get_node
):
    # Given a motor_client
    client, database, _ = await motor_client

    # And a target log file
    tgt_log_file = temp_one_line_log

    # And a display header
    header = (
        "| ObjectId\t\t\t| Node\t| Severity\t| JVM\t| Timestamp\t| "
        "Source\t| Type\t| Message\t|\n"
    )

    # And an initialized database
    try:
        await db.init(database, client)

        # And a log
        logs = await convert.convert(tgt_log_file)

        # And it has saved the log
        ids = await db.insert_logs(logs)
        id = ids.inserted_ids[0]

        # And it gets the log
        result = await db.get_log(id)

        # And it has a StringIO to capture output
        capturedOutput = io.StringIO()
        sys.stdout = capturedOutput

        # And the expected output is
        out = (
            f"{header}| {id}\t| node\t| INFO\t| jvm 1\t| "
            f"2022-07-11 09:12:02\t| ttl.test\t| SMB\t| Exec proxy\t|\n"
        )

        # When it tries to display the logs
        await view.display_result(result)

        # Then the logs are displayed
        sys.stdout = sys.__stdout__
        #
        assert capturedOutput.getvalue() == out
        # assert result == capturedOutput.getvalue()

    finally:
        await client.drop_database(database)
