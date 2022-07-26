"""
This module contains shared fixtures, steps and hooks.
"""
import pytest
import logging

from config import get_settings


@pytest.fixture()
def settings():
    return get_settings()


@pytest.fixture()
def logger(caplog):
    caplog.set_level(logging.DEBUG)
    return caplog


@pytest.fixture()
def one_line_log():
    return ("INFO | jvm | 2022/07/11 | ttl | swift | SMB | Executing haproxy")


@pytest.fixture()
def two_line_log():
    return ("INFO | jvm | 2022/07/11 | ttl | swift | SMB | Exec haproxy\n" /
            + "INFO | jvm | 2022/07/11 | ttl | swift | SMB | Exec haproxy")
