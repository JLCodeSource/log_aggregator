import asyncio
import logging
from pathlib import Path

import pytest

from aggregator.main import consume, publish
from aggregator.model import LogFile, ZipFile

logger: logging.Logger = logging.getLogger(__name__)


""" @pytest.mark.asyncio(scope="session")
@pytest.mark.mock
async def test_main(logger, settings_override, monkeypatch):

    def mock_get_settings():
        return settings_override

    monkeypatch.setattr(main, "Settings", mock_get_settings)

    asyncio.run(main())
    logs = logger.record_tuples
    assert logs[0][0] == "fail"
 """


class TestQueues:
    @pytest.mark.unit
    @pytest.mark.asyncio
    def test_create_queue_publish_structure(self) -> None:
        # Given a queue function
        # When it tries to add to the queue
        queue: asyncio.Queue[int] = asyncio.Queue()
        value: int = 1
        asyncio.run(publish(queue, value))

        # Then something is added to the queue
        assert asyncio.run(queue.get()) == 1

    @pytest.mark.unit
    @pytest.mark.asyncio
    def test_create_queue_consume_structure(self) -> None:
        # Given a queue
        queue: asyncio.Queue = asyncio.Queue()
        # When a value is added to the queue
        value: int = 1
        asyncio.run(publish(queue, value))

        # Then it can read the queue
        assert asyncio.run(consume(queue)) == 1

    @pytest.mark.unit
    @pytest.mark.asyncio
    def test_multiple_queues(self) -> None:
        # Given a queue
        zip_file_queue: asyncio.Queue = asyncio.Queue()
        # And another queue
        log_file_queue: asyncio.Queue = asyncio.Queue()

        # When a value is added to one queue
        asyncio.run(publish(zip_file_queue, 1))
        # And a different value is added to the other queue
        asyncio.run(publish(log_file_queue, 2))

        # Then the value added to the second queue is in the second queue
        assert asyncio.run(consume(log_file_queue)) == 2
        # And the value added to the first queue is in the first queue
        assert asyncio.run(consume(zip_file_queue)) == 1


class TestQueuesZip(TestQueues):
    @pytest.mark.unit
    @pytest.mark.asyncio
    def test_adding_zip_files_to_zipfile_queue(self) -> None:
        # Given a queue
        zip_file_queue: asyncio.Queue[ZipFile] = asyncio.Queue()

        # And a mock dirlist of files
        dirlist: list[ZipFile] = []
        for i in range(10):
            dirlist.append(ZipFile(fullpath=Path(f"file{i}.zip")))

        # When a list of zip files is added to it
        for zip in dirlist:
            asyncio.run(publish(zip_file_queue, zip))

        # Then the consumer can read them
        new_list: list[ZipFile] = []
        while not zip_file_queue.empty():
            zip = asyncio.run(consume(zip_file_queue))
            new_list.append(zip)
            zip_file_queue.task_done()

        assert new_list == dirlist

        # TODO: Add testing actual asyncio accessing queues!


class TestQueuesLogFile(TestQueues):
    @pytest.mark.unit
    @pytest.mark.asyncio
    def test_adding_log_files_to_logfile_queue(self) -> None:
        # Given a queue
        log_file_queue: asyncio.Queue[LogFile] = asyncio.Queue()

        # And a mock zip file
        zip_file: ZipFile = ZipFile(fullpath=Path("file.zip"))

        # And a mock dirlist of log files
        dirlist: list[LogFile] = []
        for i in range(5):
            if i == 0:
                dirlist.append(LogFile(source_zip=zip_file, fullpath=Path("file.log")))
            else:
                dirlist.append(
                    LogFile(source_zip=zip_file, fullpath=Path(f"file.log{i}"))
                )

        # When a list of log files is added to it
        for log_file in dirlist:
            asyncio.run(publish(log_file_queue, log_file))

        # Then the consumer can read them
        new_list: list[LogFile] = []
        while not log_file_queue.empty():
            log: LogFile = asyncio.run(consume(log_file_queue))
            new_list.append(log)
            log_file_queue.task_done()

        assert new_list == dirlist

        # TODO: Add testing actual asyncio accessing queues!
