import logging
import shutil
import pytest
import os
from aggregator import convert

module_name = "aggregator.convert"
multi_line_log = (
    "INFO | This is a log\nERROR | This is an error log\n    "
    "with multiple lines\n    and more lines\n"
    "INFO | And this is a separate log"
)
testdata_log_dir = "./testsource/logs/"
multi_line_log_filename = "multi_line_log.log"


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
    assert convert.line_start_match("INFO", "INFO | j |") is True

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
    assert convert.line_start_match("INFO", "xyz") is False

    # And the logger logs it
    assert logger.record_tuples == [
        (module_name, logging.DEBUG,
         "Matches: False from INFO with 'xyz'")]


@pytest.mark.unit
def test_line_start_match_non_string_arg1(logger):
    # Given a non-string to match (1)
    # When it tries to match
    # Then it raises a TypeError
    with pytest.raises(TypeError):
        convert.line_start_match(1, "xyz")

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
        convert.line_start_match("INFO", 1)

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
    log_list = list(convert.yield_matches(logs))

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
    log_list = list(convert.yield_matches(logs))

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
def test_multi_to_single_line(
        tmpdir):
    # Given a logfile with 5 lines & 3 individual logs (multi_line_log)
    log_file = os.path.join(testdata_log_dir, multi_line_log_filename)

    # And a tmpdir (tmpdir)
    # And the log_file is in the tmpdir
    shutil.copy(log_file, tmpdir)
    log_file = os.path.join(tmpdir, multi_line_log_filename)

    # When it opens the logfile
    convert.multi_to_single_line(log_file)

    # Then it converts any multiline logs into single lines
    with open(log_file, "r") as file:
        lines = len(file.readlines())
    assert lines == 3
