import logging
import os
from pathlib import Path
from typing import Literal

import pytest

from aggregator import config, main
from aggregator.config import Settings

module_name: Literal["aggregator.main"] = "aggregator.main"


class TestGetSettings:
    @pytest.mark.unit
    def test_get_settings_override(
        self,
        settings_override: Settings,
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

    @pytest.mark.unit
    def test_get_settings_from_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Given a list of env vars
        environment: str = "test"
        testing: str = "1"
        testing_out: bool = True
        connection: str = (
            "mongodb://test:test@mongoserver.domain.tld:28017//?authMechanism=DEFAULT"
        )
        connection_log: str = "mongodb://username:password@mongoserver.domain.tld:28017//?authMechanism=DEFAULT"
        sourcedir: str = "/tmp/testsource"
        outdir: str = "/tmp/outdir"
        testdatadir: str = "/tmp/testdata"
        database: str = "testdb"
        log_level: str = "50"

        # And a mock to override main._get_settings

        def mock_get_settings(*args, **kwargs) -> Settings:
            # Given a list of env values
            os.environ["ENVIRONMENT"] = environment
            os.environ["TESTING"] = testing
            os.environ["CONNECTION"] = connection
            os.environ["SOURCEDIR"] = sourcedir
            os.environ["OUTDIR"] = outdir
            os.environ["TESTDATADIR"] = testdatadir
            os.environ["DATABASE"] = database
            os.environ["LOG_LEVEL"] = log_level
            new_settings: Settings = Settings()
            return new_settings

        monkeypatch.setattr(config, "get_settings", mock_get_settings)

        # When it gets the Settings
        new_settings: Settings = main._get_settings()

        # Then the settings reflect the values
        assert new_settings.get_environment() == environment
        assert new_settings.get_testing() == testing_out
        assert new_settings.get_connection() == connection
        assert new_settings.get_connection_log() == connection_log
        assert new_settings.get_sourcedir() == Path(sourcedir)
        assert new_settings.get_outdir() == Path(outdir)
        assert new_settings.get_testdatadir() == Path(testdatadir)
        assert new_settings.get_database() == database
        assert new_settings.get_log_level() == int(log_level)
