import logging

import pytest
import convert


@pytest.fixture()
def one_line_log():
    return ("INFO | jvm | 2022/07/11 | ttl | swift | SMB | Executing haproxy")


@pytest.fixture()
def two_line_log():
    return ("INFO | jvm | 2022/07/11 | ttl | swift | SMB | Executing haproxy\nINFO | jvm | 2022/07/11 | ttl | swift | SMB | Executing haproxy")


def test_lineStartMatch_matches(logger):
    assert convert.lineStartMatch("INFO", "INFO | jvm|") is True
    assert logger.record_tuples == [("convert", logging.DEBUG,
                                     "Matches: True from INFO with 'INFO | jvm|'")]


def test_lineStartMatch_no_Match(logger):
    assert convert.lineStartMatch("INFO", "xyz") is False
    assert logger.record_tuples == [("convert", logging.DEBUG,
                                     "Matches: False from INFO with 'xyz'")]
