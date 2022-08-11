"""
This module contains shared fixtures, steps and hooks.
"""
from pathlib import Path
from random import randrange
import asyncio
import shutil
import pytest
import logging
# import motor
import string
import os
import random
from aggregator import config, convert
from aggregator.model import JavaLog
# from beanie import init_beanie
from datetime import datetime


TEST_DATABASE = "test-logs"


EXAMPLE_GEN = (  # Row 1
    (
        "INFO", "jvm 1", "2022/07/11 09:12:02", "ttl.test", "SMB", "Exec proxy"
    ),
    (  # Row 2
        "INFO", "jvm 1", "2022/07/11 09:12:55",
        "SecondaryMonitor -> {path: /path/secondary}"
    ),
    (  # Row 3
        "WARN", "jvm 1", "2022/07/11 09:13:01", "ttl.test", "async", "FileIO"
    ),
)


@pytest.helpers.register
def gen_tmp_log_dir(tmpdir: os.path,
                    target: os.path = "System"):
    Path(os.path.join(
        tmpdir, target)).mkdir(parents=True, exist_ok=True)


@pytest.helpers.register
def gen_log_file(logs: tuple[tuple[str]], log_file: os.path):
    # Given a set of log data and
    log = ""
    for row in logs:
        for field in row:
            log = f"{log} {field}\t|"
        log = f"{log}\n"
    with open(log_file, "w") as f:
        f.write(log)


@pytest.helpers.register
def gen_zip_file(log_dir: os.path,
                 zip_file: os.path,
                 target: os.path = "System"):

    if target not in log_dir:
        gen_tmp_log_dir(log_dir, target)

    log_files = []
    for log in os.listdir(log_dir):
        log_files.append(log)
        shutil.move(log, target)

    shutil.make_archive(
        zip_file, "zip", os.path.join(log_dir, target))


@pytest.helpers.register
def tmp_database():
    choices = string.ascii_lowercase + string.digits
    postfix = "".join(random.choices(choices, k=4))
    database = TEST_DATABASE
    database = f"{database}-{postfix}"
    return database


@pytest.helpers.register
def log_recorder(recorded_tuples):
    modules = []
    levels = []
    messages = []
    for recorded_log in recorded_tuples:
        modules.append(recorded_log[0])
        levels.append(recorded_log[1])
        messages.append(recorded_log[2])

    return modules, levels, messages


@pytest.helpers.register
def count_items(list, item):
    items = 0
    for element in list:
        if element == item:
            items = items + 1
    return items


class MockGetNode:

    @staticmethod
    def mock_get_node():
        return "node"


@pytest.fixture()
def mock_get_node(monkeypatch):

    def mock_get_node(*args, **kwargs):
        return MockGetNode.mock_get_node()

    monkeypatch.setattr(convert, "get_node", mock_get_node)


@pytest.fixture()
def settings_override():
    settings = config.get_settings()
    settings.database = TEST_DATABASE
    settings.log_level = logging.DEBUG
    settings.testing = True
    settings.sourcedir = "./testsource/zips"
    return settings


@pytest.fixture()
def get_datetime():
    return datetime(2022, 8, 6, 12, 1, 1)


@pytest.fixture()
async def motor_conn(settings_override):
    # Given a motor_client generator
    database = tmp_database()
    connection = settings_override.connection
    # It returns client, database & db_logname
    return database, connection


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


@pytest.fixture
def make_logs(request, tmpdir, make_filename, testdata_log_dir):
    # Given a directory (tmpdir) & a log_file
    log_files = []
    params = []
    if isinstance(request.param, str):
        params.append(request.param)
    else:
        params = request.param
    for param in params:
        file = os.path.splitext(param)
        filename = file[0]
        ext = file[1]
        log_file_name = make_filename(
            "node", filename, ext, False)[2:]
        src_log_file = os.path.join(
            testdata_log_dir, param)
        tgt_folder = os.path.join(
            tmpdir, os.path.dirname(log_file_name))
        tgt_log_file = os.path.join(
            tgt_folder, os.path.basename(log_file_name))

        # And a multi-line-log has been copied to the log_file
        os.makedirs(tgt_folder, exist_ok=True)
        shutil.copy(src_log_file, tgt_log_file)
        log_files.append(tgt_log_file)

    if len(log_files) == 1:
        return log_files[0]
    else:
        return log_files


@pytest.fixture()
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()
