import logging
from operator import mod
import pytest
from aggregator import convert

module_name = "aggregator.convert"


@pytest.mark.unit
def test_lineStartMatch_matches(logger):
    assert convert.line_start_match("INFO", "INFO | j |") is True
    assert logger.record_tuples == [
        (module_name, logging.DEBUG,
         "Matches: True from INFO with 'INFO | j |'")
    ]


@pytest.mark.unit
def test_line_start_match_no_match(logger):
    assert convert.line_start_match("INFO", "xyz") is False
    assert logger.record_tuples == [(module_name, logging.DEBUG,
                                     "Matches: False from INFO with 'xyz'")]
