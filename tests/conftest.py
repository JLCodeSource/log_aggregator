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


@pytest.fixture()
def settings_override():
    settings = config.get_settings()
    settings.database = "test-logs"
    settings.log_level = logging.DEBUG
    settings.testing = True
    settings.sourcedir = "./testsource/zips"
    return settings


@pytest.fixture()
def get_datetime():
    return datetime(2022, 8, 6, 12, 1, 1)


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
def testdata_log_dir():
    return ("./testsource/logs/")


@pytest.fixture()
def multi_line_log_filename():
    return ("multi_line_log.log")


@pytest.fixture()
def simple_svc_template_log():
    return ("simple_svc_template.log")


@pytest.fixture()
def bad_timestamp_log():
    return ("bad_timestamp.log")


@pytest.fixture()
def one_line_log():
    return ("one_line_log.log")


@pytest.fixture()
def multi_line_log():
    return("INFO | This is a log\nERROR | This is an error log\n    "
           "with multiple lines\n    and more lines\n"
           "INFO | And this is a separate log")


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


@pytest.fixture(scope="session")
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()
