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
        queue = asyncio.Queue()
        value: int = 1
        asyncio.run(publish(queue, value))
        
        # Then something is added to the queue
        assert asyncio.run(queue.get()) == 1

    
    @pytest.mark.unit
    @pytest.mark.asyncio
    def test_create_queue_consume_structure(self) -> None:
        # Given a queue function
        # When it tries to add to the queue
        queue = asyncio.Queue()
        value: int = 1
        asyncio.run(publish(queue, value))
        
        # Then something is added to the queue
        assert asyncio.run(consume(queue)) == 1



