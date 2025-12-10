"""
This module contains shared fixtures, steps and hooks.
"""

import asyncio
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from random import randrange
from typing import Any, Generator, Iterator, Union  # Literal

import pytest
from pytest_mock_resources import Credentials, create_mongo_fixture

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
    ("WARN", "jvm 1", "2022/07/11 09:13:01", "ttl.test", "async", "FileIO"),  # Row 3
)


@pytest.fixture(scope="session", autouse=True)
def faker_seed() -> int:
    return 101


@pytest.helpers.register  # type: ignore
def gen_tmp_log_dir(
    tmp_path: Path,
    target: Path = Path("System"),
) -> None:
    Path(os.path.join(str(tmp_path), str(target))).mkdir(parents=True, exist_ok=True)


@pytest.helpers.register  # type: ignore
def gen_log_file(
    faker,
    log_file: Path,
    # logs: tuple[tuple[str]],
) -> None:
    # Given a set of log data and
    levels: list[str] = ["INFO", "WARN", "ERROR"]
    jvm: list[str] = ["jvm 1"]
    timestamp: Iterator[tuple[datetime, Any]] = faker.time_series(
        start_date=datetime(2022, 8, 6, 12, 1, 1, 0),
        precision=1.0,
        end_date=datetime(2022, 8, 6, 12, 1, 1, 5),
    )
    source: list[str] = [
        "tld.main.java.cmp.api.impl.Network",
        "tld.main.java.cmp.file.server.ServiceImpl",
        "tld.main.java.cmp.file.server.LiveCheckTask",
        "tld.main.java.cmp.api.async.AsyncCacheCleaner",
        "tld.main.java.cmp.file.server.FileConfigUpdater",
        "tld.main.java.common.block.BlockingCommandExecutor",
        "tld.main.java.cmp.database.proxy.ProxyManager",
        "tld.main.java.cmp.api.cache.UserTreeCache",
        "tld.main.java.cmp.api.async.AsyncFilesHolder",
        "tld.main.dist.file.dedupe.INodeFactory",
        "tld.main.java.archivemgmt.managers.DeletionManager"
        "tld.main.java.cmp.cache.FolderUpdateCache",
        "tld.main.java.cmp.database.proxy.ProxyConfigHandler"
        "tld.main.java.cmp.api.impl.Manager",
        "tld.main.java.cmp.cache.CacheManagementTask",
    ]
    category: list[str] = [f"pool-9-thread{x}" for x in range(15)]
    category2: list[str] = [
        "AsyncCacheCleaner",
        "AsyncNetFilesHolder",
        "FolderCacheUpdateTask",
        "FileConfigUpdater",
        "FileLiveCheckTask",
        "ProxyManager",
        "CacheManagementTask",
    ]
    category.extend(category2)
    message: list[str] = []
    for _ in range(50):
        message.append(faker.sentence(nb_words=50))
    logs = faker.psv(
        data_columns=(
            {{levels}},  # type: ignore
            {{jvm}},  # type: ignore
            {{timestamp}},  # type: ignore
            {{source}},  # type: ignore
            {{category}},  # type: ignore
            {{message}},  # type: ignore
        ),
        num_rows=1000,
    )
    print(logs)
    # log: str = ""
    # for row in logs:
    #    for field in row:
    #        log = f"{log} {field}\t|"
    #    log = f"{log}\n"
    with open(log_file, "w") as f:
        f.write(logs)


@pytest.helpers.register  # type: ignore
def gen_zip_file(
    log_dir: Path,
    zip_file: Path,
    target: Path = Path("System"),
) -> None:
    if target not in os.listdir(log_dir):
        gen_tmp_log_dir(log_dir, target)

    log_files: list[Union[str, bytes, os.PathLike]] = []
    for log in os.listdir(log_dir):
        log_files.append(log)
        shutil.move(str(log), str(target))

    shutil.make_archive(str(zip_file), "zip", os.path.join(str(log_dir), str(target)))


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
            items = items + 1
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


"""
@pytest.fixture(scope="session")
def mongo():  # type: ignore
    config: MongoConfig = MongoConfig(
        image="mongo:latest",
        port=27017,
        host="127.0.0.1",
    )
    mongo = create_mongo_fixture(pmr_mongo_config=config)  # type: ignore
    return mongo
"""

mongo = create_mongo_fixture()


@pytest.fixture()
def settings_override() -> config.Settings:
    settings: config.Settings = config.get_settings()
    settings.database = "test-logs"
    settings.log_level = logging.DEBUG
    settings.testing = True
    settings.sourcedir = Path("./testsource/zips")
    return settings


@pytest.fixture()
def motor_conn(mongo) -> tuple[str, str]:
    creds: Credentials = mongo.pmr_credentials
    database: str = creds.database
    conn: str = creds.as_url()
    return database, conn


@pytest.fixture()
def get_datetime() -> datetime:
    return datetime(2022, 8, 6, 12, 1, 1)


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
def make_filename(settings_override: config.Settings, tmp_path: Path) -> object:
    settings: config.Settings = settings_override

    def _make_filename(node: str, service: str, ext: str, tld: bool) -> Path | None:
        ts: int = 1660736299000 + randrange(-100000000, 100000000)
        outdir: str = os.path.join(tmp_path, os.path.relpath(settings.outdir))
        if ext == ".zip" and tld is True:
            filename: str = os.path.join(
                outdir, f"GBLogs_{node}.domain.tld_{service}_{ts}.zip"
            )
        elif ext == ".zip" and tld is False:
            filename = os.path.join(outdir, f"GBLogs_{node}_{service}_{ts}.zip")
        elif ext == ".log":
            file: str = f"{service}{ext}"

            filename = os.path.join(outdir, node, service, file)
        else:
            return None
        return Path(filename)

    return _make_filename


@pytest.fixture
def make_logs(
    request, tmp_path: Path, make_filename, testdata_log_dir: Path
) -> Path | list[Path]:
    # Given a directory (tmpdir) & a log_file
    log_files: list[Path] = []
    params: list[str] = []
    if isinstance(request.param, str):
        params.append(request.param)
    else:
        params = request.param
    for param in params:
        file: tuple[str, str] = os.path.splitext(str(param))
        filename: str = file[0]
        ext: str = file[1]
        log_file_name: str | None = make_filename("node", filename, ext, False)
        if log_file_name is None:
            continue
        src_log_file: str = os.path.join(testdata_log_dir, param)
        tgt_folder: Path = Path(os.path.join(tmp_path, os.path.dirname(log_file_name)))
        tgt_log_file: Path = Path(
            os.path.join(tgt_folder, os.path.basename(log_file_name))
        )

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
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()
