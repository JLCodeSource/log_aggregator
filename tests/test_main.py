import logging
from typing import Literal

import pytest

from aggregator import config, main
from aggregator.config import Settings

module_name: Literal["aggregator.main"] = "aggregator.main"


class TestGetSettings:
    @pytest.mark.mock
    def test_get_settings_override(
        self,
        settings_override: Settings,
        monkeypatch: pytest.MonkeyPatch,
        logger: pytest.LogCaptureFixture,
    ) -> None:

        # Given a settings (settings_override)
        # When it tries to get the settings
        settings: Settings = main._get_settings()

        # Then the settings will be overrideen
        assert settings == settings_override

        # And it logs the settings
        mods: list[str] = []
        lvls: list[int] = []
        msgs: list[str] = []
        mods, lvls, msgs = pytest.helpers.log_recorder(  # type: ignore
            logger.record_tuples
        )

        assert all(mod == module_name for mod in mods)
        assert lvls[0] and lvls[-1] == logging.INFO
        assert all(lvl == logging.DEBUG for lvl in lvls[1:-2])
        assert msgs[0] == "Loading config settings from the environment..."
        assert msgs[1] == "Environment: dev"
        assert msgs[2] == "Testing: True"
        assert (
            msgs[3]
            == "Connection: mongodb://username:password@localhost:27017/?authMechanism=DEFAULT"
        )
        assert msgs[4] == "Sourcedir: testsource/zips"
        assert msgs[5] == "Outdir: out"
        assert msgs[6] == "Database: test-logs"
        assert msgs[7] == "Log Level: 10"

    @pytest.mark.mock
    def test_get_settings_empty(
        self, monkeypatch: pytest.MonkeyPatch, logger: pytest.LogCaptureFixture
    ) -> None:

        # Given a mock get_settings
        def mock_get_settings(*args, **kwargs) -> None:
            return None

        monkeypatch.setattr(config, "get_settings", mock_get_settings)

        # When it tries to get the settings
        with pytest.raises(AssertionError):
            settings: Settings | None = main._get_settings()

            # And the settings are None
            assert settings is None

            # And it logs it
            assert logger.record_tuples[0] == [
                module_name,
                logging.FATAL,
                "AssertionError: Failed to get settings",
            ]
