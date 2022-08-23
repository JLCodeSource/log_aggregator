import logging
import os
from pathlib import Path
from typing import Any, Coroutine, Literal

import pytest
from motor.motor_asyncio import AsyncIOMotorClient

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
            msgs[3] ==
            "Connection: mongodb://username:password@localhost:27017/?authMechanism=DEFAULT"
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

    @pytest.mark.unit
    def test_settings_failed(
        self, monkeypatch: pytest.MonkeyPatch, logger: pytest.LogCaptureFixture
    ) -> None:
        # Given a mock get settings
        def mock_get_settings(*args, **kwargs) -> None:
            return None

        monkeypatch.setattr(config, "get_settings", mock_get_settings)

        # When it gets the settings
        # Then it raises an assertion error
        with pytest.raises(AssertionError):
            main._get_settings()

        # And the logger logs it
        assert logger.record_tuples[0] == (
            module_name,
            logging.FATAL,
            "AssertionError: Failed to get settings",
        )


class TestInit:
    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_init_db_success(self) -> None:
        # When main tries to init the db
        # Then it returns the client
        client: AsyncIOMotorClient = await main._init_db()
        assert isinstance(client, AsyncIOMotorClient)

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_init_app_success(self, settings_override) -> None:
        # When main tries to init the app
        # Then it returns settings and client
        client: AsyncIOMotorClient
        settings: Settings
        client, settings = await main.init_app()
        assert isinstance(client, AsyncIOMotorClient)
        assert settings == settings_override

    # TODO: Add failure tests (though bunnying off other tests)


class TestExtract:
    @pytest.mark.unit
    def test_get_zip_extract_coro_list(
        self, settings_override: config.Settings
    ) -> None:
        # Given a set of settings (settings_override)
        # When it tries to generate the list
        zip_coro_list: list[
            Coroutine[Any, Any, list[Path]]
        ] = main._get_zip_extract_coro_list(settings_override)
        # Then it returns a list of coros
        assert len(zip_coro_list) > 0

    @pytest.mark.unit
    def test_get_zip_extract_coro_list_is_none_or_empty(
        self,
        settings_override: config.Settings,
        tmp_path: Path,
        logger: pytest.LogCaptureFixture,
    ) -> None:
        # Given a set of settings (settings_override)
        # And a tmp_path
        settings_override.sourcedir = tmp_path
        # When it tries to generate the list
        # Then it raises a ValueError
        with pytest.raises(ValueError):
            main._get_zip_extract_coro_list(settings_override)
        # And the logger logs it
        assert logger.record_tuples[0] == (
            module_name,
            logging.ERROR,
            "ValueError: Zip extract coroutine list is empty",
        )
