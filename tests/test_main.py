from main import main
import asyncio
import pytest
import logging
from config import Settings


logger: logging.Logger = logging.getLogger(__name__)


@pytest.mark.asyncio()
@pytest.mark.mock
async def test_main(logger, settings_override, monkeypatch) -> None:

    def mock_get_settings() -> Settings:
        return settings_override

    monkeypatch.setattr(main, "Settings", mock_get_settings)

    asyncio.run(main())
    logs = logger.record_tuples
    assert logs[0][0] == "fail"
