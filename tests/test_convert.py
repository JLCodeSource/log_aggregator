from datetime import datetime
import logging
import shutil
import pytest
import os
from aggregator import convert, db
from beanie.exceptions import CollectionWasNotInitialized

module_name = "aggregator.convert"
multi_line_log = (
    "INFO | This is a log\nERROR | This is an error log\n    "
    "with multiple lines\n    and more lines\n"
    "INFO | And this is a separate log"
)
testdata_log_dir = "./testsource/logs/"
multi_line_log_filename = "multi_line_log.log"
simple_svc_template_log = "simple_svc_template.log"
bad_timestamp_log = "bad_timestamp.log"
one_line_log = "one_line_log.log"


class MockOpen:
    # Mock Open

    @staticmethod
    def read_multi(file):
        return multi_line_log


@pytest.mark.unit
def test_lineStartMatch_matches(logger):
    # Given a string to match (INFO)
    # And a string that matches (INFO | j |)
    # When it tries to match
    # Then it matches
    assert convert._line_start_match("INFO", "INFO | j |") is True

    # And the logger logs it
    assert logger.record_tuples == [
        (module_name, logging.DEBUG,
         "Matches: True from INFO with 'INFO | j |'")
    ]


@pytest.mark.unit
def test_line_start_match_no_match(logger):
    # Given a string to match (INFO)
    # And a string that doesn't matches (xyz)
    # When it tries to match
    # Then it doesn't match
    assert convert._line_start_match("INFO", "xyz") is False

    # And the logger logs it
    assert logger.record_tuples == [
        (module_name, logging.DEBUG,
         "Matches: False from INFO with 'xyz'")
    ]


@pytest.mark.unit
def test_line_start_match_non_string_arg1(logger):
    # Given a non-string to match (1)
    # When it tries to match
    # Then it raises a TypeError
    with pytest.raises(TypeError):
        convert._line_start_match(1, "xyz")

    # And logs a warning
    assert logger.record_tuples[0] == (
        module_name, logging.WARNING,
        "TypeError: first argument must be string or compiled pattern"
    )


@pytest.mark.unit
def test_line_start_match_non_string_arg2(logger):
    # Given a non-string to match (1)
    # When it tries to match
    # Then it raises a TypeError
    with pytest.raises(TypeError):
        convert._line_start_match("INFO", 1)

    # And logs a warning
    assert logger.record_tuples[0] == (
        module_name, logging.WARNING,
        "TypeError: expected string or bytes-like object"
    )


@pytest.mark.unit
def test_yield_matches_one_line(logger):
    # Given 2 single line logs in a list of logs that starts with INFO
    logs = "INFO | This is a log\nINFO | This is another log"

    # When it tries to match the lines
    log_list = list(convert._yield_matches(logs))

    # Then the first log is yielded
    assert log_list[0] == "INFO | This is a log"

    # And the second log is yielded
    assert log_list[1] == "INFO | This is another log"

    # And the logger logs it
    assert logger.record_tuples[1] == (
        module_name, logging.DEBUG,
        f"Appended: {log_list[0]} to list"
    )
    assert logger.record_tuples[3] == (
        module_name, logging.DEBUG,
        f"Appended: {log_list[1]} to list"

    )


@pytest.mark.unit
def test_yield_matches_multi_line(logger):
    # Given a multiline error log that starts with ERROR
    logs = multi_line_log
    # And the logs split by line
    lines = logs.split("\n")

    # When it tries to match the lines
    log_list = list(convert._yield_matches(logs))

    # Then the first log is yielded
    assert log_list[0] == "INFO | This is a log"

    # And the multi-line log is yielded
    assert log_list[1] == (
        "ERROR | This is an error log; with multiple lines; "
        "and more lines"
    )

    # And the single line log at the end is yielded
    assert log_list[2] == "INFO | And this is a separate log"

    # And the logger logs it
    modules = []
    levels = []
    messages = []
    for recorded_log in logger.record_tuples:
        modules.append(recorded_log[0])
        levels.append(recorded_log[1])
        messages.append(recorded_log[2])

    # And the logger logs modules & levels
    assert all(module == module_name for module in modules)
    assert all(level == logging.DEBUG for level in levels)

    # And the logger logs lines
    for line in lines:
        assert any(
            message == f"Appended: {line} to list" for
            message in messages
        )


@pytest.mark.unit
def test_multi_to_single_line(tmpdir):
    # Given a logfile with 5 lines & 3 individual logs (multi_line_log)
    log_file = os.path.join(testdata_log_dir, multi_line_log_filename)

    # And a tmpdir (tmpdir)
    # And the log_file is in the tmpdir
    shutil.copy(log_file, tmpdir)
    log_file = os.path.join(tmpdir, multi_line_log_filename)

    # When it opens the logfile
    convert._multi_to_single_line(log_file)

    # Then it converts any multiline logs into single lines
    with open(log_file, "r") as file:
        lines = len(file.readlines())
    assert lines == 3

    # And it logs it
    # TODO: These checks

# TODO: Add unhappy paths


@pytest.mark.unit
def test_convert_log_to_csv_success(tmpdir):
    # Given a logfile with 5 lines & 3 individual logs (multi_line_log)
    log_file = os.path.join(testdata_log_dir, multi_line_log_filename)

    # And a tmpdir (tmpdir)
    # And the log_file is in the tmpdir
    shutil.copy(log_file, tmpdir)
    log_file = os.path.join(tmpdir, multi_line_log_filename)

    # And it has converted the file to single lines
    convert._multi_to_single_line(log_file)

    # When it tries to convert the CSV log file to a dict
    result = convert._convert_log_to_csv(log_file)

    # Then it succeeds
    assert type(result) == list
    assert type(result[0]) == dict

    # TODO: Improve checks
    # And add logging

# TODO: Add unhappy paths


class MockGetNode:

    @staticmethod
    def mock_get_node():
        return "node"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_convert_collection_not_initialized(
        tmpdir, make_filename, monkeypatch,
        logger):
    # TODO: This test is brittle as it is dependent on being called
    # before convert_success

    # Given a directory (tmpdir) & a log_file
    log_file_name = make_filename("node", "service", ".log", False)[2:]
    src_log_file = os.path.join(testdata_log_dir, one_line_log)
    tgt_folder = os.path.join(tmpdir, os.path.dirname(log_file_name))
    tgt_log_file = os.path.join(tgt_folder, os.path.basename(log_file_name))

    # And a multi-line-log has been copied to the log_file
    os.makedirs(tgt_folder, exist_ok=True)
    shutil.copy(src_log_file, tgt_log_file)

    # And a Mock get_node
    def mock_helper_get_node(*args, **kwargs):
        return MockGetNode.mock_get_node()

    monkeypatch.setattr(convert, "get_node", mock_helper_get_node)

    # When it tries to convert the logs
    # Then it raises a CollectionNotInitialized error
    with pytest.raises(CollectionWasNotInitialized):
        await convert.convert(tgt_log_file)

    # Then it logs an AttributeError:
    assert logger.record_tuples[7][0] == module_name
    assert logger.record_tuples[7][1] == logging.CRITICAL
    assert logger.record_tuples[7][2] == (
        "Error: err=CollectionWasNotInitialized(), type(err)=<class "
        "'beanie.exceptions.CollectionWasNotInitialized'>"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_convert_success(
        tmpdir, make_filename, monkeypatch,
        motor_client_gen):
    # Given a directory (tmpdir) & a log_file
    log_file_name = make_filename("node", "service", ".log", False)[2:]
    src_log_file = os.path.join(testdata_log_dir, simple_svc_template_log)
    tgt_folder = os.path.join(tmpdir, os.path.dirname(log_file_name))
    tgt_log_file = os.path.join(tgt_folder, os.path.basename(log_file_name))

    # And a multi-line-log has been copied to the log_file
    os.makedirs(tgt_folder, exist_ok=True)
    shutil.copy(src_log_file, tgt_log_file)

    # And a Mock get_node
    def mock_helper_get_node(*args, **kwargs):
        return MockGetNode.mock_get_node()

    monkeypatch.setattr(convert, "get_node", mock_helper_get_node)

    # And a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # And a mocked database name output for logs
    # database_log_name = settings_override.database

    # And an initialized database
    try:
        await db.init(database, client)

        # When it tries to convert the logs
        log_list = await convert.convert(tgt_log_file)

        # Then it succeeds
        assert len(log_list) == 5
        assert all(log.node == "node" for log in log_list)
        assert all(log.jvm == "jvm 1" for log in log_list)
        assert sum(log.severity == "INFO" for log in log_list) == 3
        assert sum(log.severity == "ERROR" for log in log_list) == 1
        assert sum(log.severity == "WARN" for log in log_list) == 1
        assert sum(log.source == "ttl.test" for log in log_list) == 3
        assert sum(log.source == "org.connect" for log in log_list) == 1
        assert all(log.type in ("SMB", "async", "event", "process", None)
                   for log in log_list)
        assert all(log.datetime in (
            datetime(2022, 7, 11, 9, 12, 2),
            datetime(2022, 7, 11, 9, 12, 55),
            datetime(2022, 7, 11, 9, 13, 1),
            datetime(2022, 7, 11, 9, 14, 51),
            datetime(2022, 7, 11, 9, 15, 51)
        ) for log in log_list)
        assert all(log.message in (
            "Exec proxy", "FileIO", "more messages",
            "SecondaryMonitor -> {path: /path/secondary, number: 2361852362752}",
            "error doing reconnect...; java.io.IOException: org.Exception: "
            "ErrorCode = Connection for /locks; "
            "at ttl.test.create(lock.java:2); "
            "at ttl.test.reconnect(lock.java:99); "
            "at ttl.test.process(lock.java:101); "
            "at org.processEvent(connect.java:500); "
            "at org.run(connect.java:200); "
            "Caused by: org.Exception: ErrorCode = Connection for /locks; "
            "at org.Exception.create(Exception.java:122); "
            "at org.Exception.create(Exception.java:540); "
            "at org.exists(exists.java:2000); "
            "at org.exists(exists.java:2079); "
            "at ttl.test.create(Lock.java:720); "
            "... 4 more"
        ) for log in log_list)

    finally:
        # Set manual teardown
        await client.drop_database(database)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_convert_to_datetime_bad_timestamp(
        tmpdir, make_filename, monkeypatch,
        motor_client_gen, logger):
    # Given a directory (tmpdir) & a log_file
    log_file_name = make_filename("node", "service", ".log", False)[2:]
    src_log_file = os.path.join(testdata_log_dir, bad_timestamp_log)
    tgt_folder = os.path.join(tmpdir, os.path.dirname(log_file_name))
    tgt_log_file = os.path.join(tgt_folder, os.path.basename(log_file_name))

    # And a multi-line-log has been copied to the log_file
    os.makedirs(tgt_folder, exist_ok=True)
    shutil.copy(src_log_file, tgt_log_file)

    # And a Mock get_node
    def mock_helper_get_node(*args, **kwargs):
        return MockGetNode.mock_get_node()

    monkeypatch.setattr(convert, "get_node", mock_helper_get_node)

    # And a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # And a mocked database name output for logs
    # database_log_name = settings_override.database

    # And an initialized database
    try:
        await db.init(database, client)

        # When it tries to convert the logs
        await convert.convert(tgt_log_file)

        # Then it logs an exception
        assert logger.record_tuples[10] == (
            module_name, logging.ERROR,
            "ValueError: time data '2022/07/1x 09:12:02' "
            "does not match format '%Y/%m/%d %H:%M:%S'"
        )

    finally:
        # Set manual teardown
        await client.drop_database(database)


class MockDatetime:

    @staticmethod
    def bad_timestamp():
        return "2022/07/1x 09:12:02"

# TODO: Test for file with trailing empty line


@pytest.mark.unit
@pytest.mark.asyncio
async def test_convert_bad_timestamp(
        tmpdir, make_filename, monkeypatch,
        motor_client_gen, logger):
    # Given a directory (tmpdir) & a log_file
    log_file_name = make_filename("node", "service", ".log", False)[2:]
    src_log_file = os.path.join(testdata_log_dir, bad_timestamp_log)
    tgt_folder = os.path.join(tmpdir, os.path.dirname(log_file_name))
    tgt_log_file = os.path.join(tgt_folder, os.path.basename(log_file_name))

    # And a multi-line-log has been copied to the log_file
    os.makedirs(tgt_folder, exist_ok=True)
    shutil.copy(src_log_file, tgt_log_file)

    # And a Mock get_node
    def mock_helper_get_node(*args, **kwargs):
        return MockGetNode.mock_get_node()

    monkeypatch.setattr(convert, "get_node", mock_helper_get_node)

    # And a mock _convert_to_datetime
    def mock_convert_to_datetime(*args, **kwargs):
        return MockDatetime.bad_timestamp()

    monkeypatch.setattr(
        convert, "_convert_to_datetime", mock_convert_to_datetime)

    # And a motor_client generator
    motor_client = await motor_client_gen
    # And a motor_client
    client = motor_client[0][0]
    # And a database
    database = motor_client[0][1]

    # And a mocked database name output for logs
    # database_log_name = settings_override.database

    # And an initialized database
    try:
        await db.init(database, client)

        # When it tries to convert the logs:
        await convert.convert(tgt_log_file)

        # Then it logs an AttributeError:
        assert logger.record_tuples[10][0] == module_name
        assert logger.record_tuples[10][1] == logging.ERROR
        assert logger.record_tuples[10][2].startswith(
            "Error <class 'pydantic.error_wrappers.ValidationError'> "
            "1 validation error for JavaLog"
        )

    finally:
        # Set manual teardown
        await client.drop_database(database)
