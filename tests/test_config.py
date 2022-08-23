import logging
import os
from pathlib import Path

import pytest

from aggregator.config import Settings

settings: Settings = Settings()

environment: str = "test"
testing: str = "1"
connection: str = (
    "mongodb://test:test@mongoserver.domain.tld:28017/?authMechanism=DEFAULT"
)
connection_log: str = (
    "mongodb://username:password@mongoserver.domain.tld:28017/?authMechanism=DEFAULT"
)
sourcedir: str = "/tmp/testsource"
outdir: str = "/tmp/outdir"
testdatadir: str = "/tmp/testdata"
database: str = "testdb"
log_level: str = "50"


@pytest.mark.unit
def test_settings_get_environment(settings_override: Settings) -> None:
    # Given a set of settings (settings_override)
    # When you check the environment value
    env: str = settings_override.get_environment()

    # Then it returns the environment setting
    assert env == "dev"


@pytest.mark.unit
def test_settings_get_testing_from_test_conf(settings_override: Settings) -> None:
    # Given a set of settings (settings_override)
    # When you check the environment value
    env: bool = settings_override.get_testing()

    # Then it returns the env setting
    assert env is True


@pytest.mark.parametrize(
    "func, value",
    [
        (settings.get_environment(), "dev"),
        (settings.get_testing(), False),
        (
            settings.get_connection(),
            "mongodb://root:example@localhost:27017/?authMechanism=DEFAULT",
        ),
        (
            settings.get_connection_log(),
            "mongodb://username:password@localhost:27017/?authMechanism=DEFAULT",
        ),
        (settings.get_sourcedir(), Path("./testsource/prod_zips")),
        (settings.get_outdir(), Path("./out")),
        (settings.get_testdatadir(), Path("./testsource")),
        (settings.get_database(), "logs"),
        (settings.get_log_level(), logging.INFO),
    ],
)
@pytest.mark.unit
def test_settings_funcs(func: object, value: str | bool | int | Path) -> None:
    assert func == value


@pytest.mark.unit
def test_settings_get_environments() -> None:
    # Given that the environment var for environment has been set
    os.environ["ENVIRONMENT"] = environment
    os.environ["TESTING"] = testing
    os.environ["CONNECTION"] = connection
    os.environ["SOURCEDIR"] = sourcedir
    os.environ["OUTDIR"] = outdir
    os.environ["TESTDATADIR"] = testdatadir
    os.environ["DATABASE"] = database
    os.environ["LOG_LEVEL"] = log_level
    # When the settings are set
    env_settings: Settings = Settings()

    # Then it returns the environment var
    assert env_settings.get_environment() == environment
    assert env_settings.get_testing() is True
    assert env_settings.get_connection() == connection
    assert env_settings.get_connection_log() == connection_log
    assert env_settings.get_sourcedir() == Path(sourcedir)
    assert env_settings.get_outdir() == Path(outdir)
    assert env_settings.get_testdatadir() == Path(testdatadir)
    assert env_settings.get_database() == database
    assert env_settings.get_log_level() == int(log_level)


@pytest.mark.unit
def test_connection_for_no_user_id() -> None:
    # Given a simple connectoin
    os.environ["CONNECTION"] = "mongodb://localhost:27017"

    # When the settings are set
    env_settings: Settings = Settings()

    # Then the connection log string is the same
    assert env_settings.get_connection_log() == env_settings.get_connection()
