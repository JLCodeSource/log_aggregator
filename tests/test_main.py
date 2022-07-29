from main import Aggregator, main
import pytest


@pytest.mark.unit
@pytest.mark.asyncio(scope="session")
async def test_main(logger, event_loop):
    event_loop.run_until_complete(main())
    yield logger
    logs = logger.record_tuples

    assert logs[0][0] == "fail"
