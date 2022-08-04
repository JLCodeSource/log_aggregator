"""
This module contains shared fixtures, steps and hooks.
"""
from random import randrange
import asyncio
import pytest
import logging
import motor
import string
import os
import random
from aggregator import config
from aggregator.model import JavaLog
# from beanie import init_beanie
from datetime import datetime


@pytest.fixture(scope="session", autouse=True)
def settings_override():
    settings = config.get_settings()
    settings.database = "test-logs"
    settings.log_level = logging.DEBUG
    settings.testing = True
    settings.sourcedir = "./testsource/logs"
    return settings


@pytest.fixture()
async def motor_client_values(settings_override):
    choices = string.ascii_lowercase + string.digits
    postfix = "".join(random.choices(choices, k=4))
    client = motor.motor_asyncio.AsyncIOMotorClient(
        settings_override.get_connection())
    database = settings_override.get_database()
    database = f"{database}-{postfix}"

    test_motor_client = [client, database]
    yield test_motor_client

    client.drop_database(database)


@pytest.fixture()
async def motor_client_gen(motor_client_values):
    return [i async for i in motor_client_values]


@pytest.fixture()
async def add_one():

    # And adds a log
    log = JavaLog(
        node="testnode",
        severity="INFO",
        jvm="jvm",
        datetime=datetime.now(),
        source="source",
        type="fanapiservice",
        message="This is a log"
    )
    await JavaLog.insert_one(log)
    await asyncio.sleep(1)


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
