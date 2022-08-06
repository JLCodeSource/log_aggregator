import logging
import pytest
from aggregator import convert

module_name = "aggregator.convert"


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


# @pytest.mark.unit
# def test_yield_matches_one_line(logger):
