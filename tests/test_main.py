from main import main
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
