"""
This module contains shared fixtures, steps and hooks.
"""
from random import randrange
import pytest
import logging
import os
from aggregator import config


@pytest.fixture()
def settings_override():
    settings = config.get_settings()
    settings.database = "test-logs"
    settings.log_level = logging.DEBUG
    settings.testing = True
    settings.sourcedir = "./testsource/logs"
    return settings


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


@pytest.fixture()
def make_filename(settings_override):

    settings = settings_override

    def _make_filename(node, service, ext, tld):
        ts = 1658844081 + randrange(-100000, 100000)

        if ext == ".zip" and tld is True:
            filename = f"GBLogs_{node}.domain.tld_" \
                + f"{service}_{ts}.zip"
        elif ext == ".zip" and tld is False:
            filename = f"GBLogs_{node}_" \
                + f"{service}_{ts}.zip"
        elif ext == ".log":
            file = f"{service}{ext}"
            filename = os.path.join(settings.outdir, node, service, file)
        return str(filename)

    return _make_filename
