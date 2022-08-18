from aggregator.main import consume, publish
import pytest
import logging
import asyncio

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
        queue = asyncio.Queue()
        # When a value is added to the queue
        value: int = 1
        asyncio.run(publish(queue, value))

        # Then it can read the queue
        assert asyncio.run(consume(queue)) == 1

    @pytest.mark.unit
    @pytest.mark.asyncio
    def test_multiple_queues(self) -> None:
        # Given a queue
        zip_file_queue = asyncio.Queue()
        # And another queue
        log_file_queue = asyncio.Queue()

        # When a value is added to one queue
        asyncio.run(publish(zip_file_queue, 1))
        # And a different value is added to the other queue
        asyncio.run(publish(log_file_queue, 2))

        # Then the value added to the second queue is in the second queue
        assert asyncio.run(consume(log_file_queue)) == 2
        # And the value added to the first queue is in the first queue
        assert asyncio.run(consume(zip_file_queue)) == 1


    @pytest.mark.unit
    @pytest.mark.asyncio
    def test_adding_files_to_queue(self) -> None:
        # Given a queue
        zip_file_queue: asyncio.Queue[str] = asyncio.Queue()

        # And a mock dirlist of files
        dirlist: list[str] = []
        for i in range(10):
            dirlist.append(f"file{i}.zip")

        # When a list of zip files is added to it
        for file in dirlist:
            asyncio.run(publish(zip_file_queue, file))

        # Then the consumer can read them
        new_list: list[str] = []
        while True:
            file: str = asyncio.run(consume(zip_file_queue))
            if file is None:
                break
            else:
                new_list.append(file)

        assert new_list == dirlist
