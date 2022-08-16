"""
This module contains shared fixtures, steps and hooks.
"""
import asyncio
import logging
import os
import random
import shutil
import string
from datetime import datetime
from ipaddress import IPv4Address
from pathlib import Path
from random import randrange
from typing import Any, Generator, Union  # Literal

import pytest
from pytest_mock_resources.container.mongo import MongoConfig
from pytest_mock_resources.fixture.database.generic import Credentials
from typing_extensions import LiteralString

from aggregator import config, convert
from aggregator.model import JavaLog

TEST_DATABASE: str = "test-logs"

EXAMPLE_GEN = (  # Row 1
    ("INFO", "jvm 1", "2022/07/11 09:12:02", "ttl.test", "SMB", "Exec proxy"),
    (  # Row 2
        "INFO",
        "jvm 1",
        "2022/07/11 09:12:55",
        "SecondaryMonitor -> {path: /path/secondary}",
    ),
    ("WARN", "jvm 1", "2022/07/11 09:13:01",
     "ttl.test", "async", "FileIO"),  # Row 3
)


@pytest.helpers.register  # type: ignore
def gen_tmp_log_dir(
    tmpdir: Union[str, bytes, os.PathLike],
    target: Union[str, bytes, os.PathLike] = "System",
) -> None:
    Path(os.path.join(str(tmpdir), str(target))).mkdir(
        parents=True, exist_ok=True)


@pytest.helpers.register  # type: ignore
def gen_log_file(
    logs: tuple[tuple[str]], log_file: Union[str, bytes, os.PathLike]
) -> None:
    # Given a set of log data and
    log: str = ""
    for row in logs:
        for field in row:
            log: str = f"{log} {field}\t|"
        log: str = f"{log}\n"
    with open(log_file, "w") as f:
        f.write(log)


@pytest.helpers.register  # type: ignore
def gen_zip_file(
    log_dir: Union[str, bytes, os.PathLike],
    zip_file: Union[str, bytes, os.PathLike],
    target: Union[str, bytes, os.PathLike] = "System",
) -> None:

    if target not in os.listdir(log_dir):
        gen_tmp_log_dir(log_dir, target)

    log_files: list[Union[str, bytes, os.PathLike]] = []
    for log in os.listdir(log_dir):
        log_files.append(log)
        shutil.move(str(log), str(target))

    shutil.make_archive(str(zip_file), "zip",
                        os.path.join(str(log_dir), str(target)))


@pytest.helpers.register  # type: ignore
def log_recorder(recorded_tuples) -> tuple[list[str], list[int], list[str]]:
    modules: list[str] = []
    levels: list[int] = []
    messages: list[str] = []
    for recorded_log in recorded_tuples:
        modules.append(recorded_log[0])
        levels.append(recorded_log[1])
        messages.append(recorded_log[2])

    return modules, levels, messages


@pytest.helpers.register  # type: ignore
def count_items(list, item) -> int:
    items: int = 0
    for element in list:
        if element == item:
            items: int = items + 1
    return items


class MockGetNode:
    @staticmethod
    def mock_get_node() -> str:
        return "node"


@pytest.fixture()
def mock_get_node(monkeypatch) -> None:
    def mock_get_node(*args, **kwargs) -> str:
        return MockGetNode.mock_get_node()

    monkeypatch.setattr(convert, "get_node", mock_get_node)


@pytest.fixture(scope="session")
def pmr_mongo_config() -> MongoConfig:
    return MongoConfig(image="mongo:latest", host="127.0.0.1")


@pytest.fixture(scope="session")
def pmr_creds() -> dict[str, Any]:
    creds: dict[str, Any] = Credentials(
        host="127.0.0.1",
        port=28017,
        drivername="mongodb",
        database="mongo-dev",
        username="",
        password="",
    ).as_mongo_kwargs()
    return dict(creds)


@pytest.fixture()
def tmp_database() -> str:
    choices: LiteralString = string.ascii_lowercase + string.digits
    postfix: str = "".join(random.choices(choices, k=4))
    database: str = TEST_DATABASE
    database: str = f"{database}-{postfix}"
    return database


@pytest.fixture()
def settings_override(tmp_database) -> config.Settings:

    settings: config.Settings = config.get_settings()
    settings.database = tmp_database
    settings.log_level = logging.DEBUG
    settings.testing = True
    settings.sourcedir = "./testsource/zips"
    return settings


@pytest.fixture()
def settings_override_pmr(
    settings_override: config.Settings, pmr_creds: dict[str, Any]
) -> tuple[config.Settings, dict[str, Any]]:
    # username = pmr_mongo_credentials.username
    # password = pmr_mongo_credentials.password
    host: IPv4Address = IPv4Address("127.0.0.1")
    port: int = 28017
    database: str = pmr_creds["database"]
    authsource: str = pmr_creds["database"]
    settings: config.Settings = settings_override
    settings.connection = (
        f"mongodb://"
        f"{host}:{port}/?authMechanism=DEFAULT&"
        f"authSource={authsource}"
    )
    settings.database = database
    return settings, pmr_creds


@pytest.fixture()
def get_datetime() -> datetime:
    return datetime(2022, 8, 6, 12, 1, 1)


@pytest.fixture()
def motor_conn(settings_override: config.Settings) -> tuple[str, str]:
    # Given a motor_client generator
    # database = tmp_database()
    database: str = settings_override.database
    connection: str = settings_override.connection
    # It returns client, database & db_logname
    return database, connection


@pytest.fixture()
async def add_one() -> JavaLog | None:

    # And adds a log
    log: JavaLog = JavaLog(
        node="testnode",
        severity="INFO",
        jvm="jvm",
        datetime=datetime.now(),
        source="source",
        type="fanapiservice",
        message="This is a log",
    )
    result: JavaLog | None = await JavaLog.insert_one(log)
    return result


@pytest.fixture()
def logger(caplog) -> pytest.LogCaptureFixture:
    caplog.set_level(logging.DEBUG)
    return caplog


@pytest.fixture()
def testdata_log_dir() -> str:
    return "./testsource/logs/"


@pytest.fixture()
def multi_line_log() -> str:
    return (
        "INFO | This is a log\nERROR | This is an error log\n    "
        "with multiple lines\n    and more lines\n"
        "INFO | And this is a separate log"
    )


@pytest.fixture()
def make_filename(settings_override: config.Settings) -> object:

    settings: config.Settings = settings_override

    def _make_filename(node: str, service: str, ext: str, tld: bool) -> str | None:
        ts: int = 1658844081 + randrange(-100000, 100000)

        if ext == ".zip" and tld is True:
            filename: str = f"GBLogs_{node}.domain.tld_" f"{service}_{ts}.zip"
        elif ext == ".zip" and tld is False:
            filename: str = f"GBLogs_{node}_" f"{service}_{ts}.zip"
        elif ext == ".log":
            file: str = f"{service}{ext}"
            filename: str = os.path.join(settings.outdir, node, service, file)
        else:
            return None
        return str(filename)

    return _make_filename


@pytest.fixture
def make_logs(request, tmpdir, make_filename, testdata_log_dir) -> str | list[str]:
    # Given a directory (tmpdir) & a log_file
    log_files: list[str] = []
    params: list[str] = []
    if isinstance(request.param, str):
        params.append(request.param)
    else:
        params: list[str] = request.param
    for param in params:
        file: tuple[str, str] = os.path.splitext(param)
        filename: str = file[0]
        ext: str = file[1]
        log_file_name: str | None = make_filename(
            "node", filename, ext, False)[2:]
        if log_file_name is None:
            continue
        src_log_file: str = os.path.join(testdata_log_dir, param)
        tgt_folder: str = os.path.join(tmpdir, os.path.dirname(log_file_name))
        tgt_log_file: str = os.path.join(
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
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    try:
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    except RuntimeError:
        loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    yield loop
    loop.close()
