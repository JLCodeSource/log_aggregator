import pytest
import logging
import os

from aggregator.config import Settings

settings: Settings = Settings()


@pytest.mark.unit
def test_settings_get_environment(
        settings_override: Settings) -> None:
    # Given a set of settings (settings_override)
    # When you check the environment value
    env: str = settings_override.get_environment()

    # Then it returns the environment setting
    assert env == "dev"


def test_settings_get_testing_from_test_conf(
        settings_override: Settings) -> None:
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
        (settings.get_connection(),
         "mongodb://root:example@localhost:27017/?authMechanism=DEFAULT"),
        (settings.get_connection_log(),
         "mongodb://username:password@localhost:27017/?authMechanism=DEFAULT"),
        (settings.get_sourcedir(), "./testsource/zips"),
        (settings.get_outdir(), "./out"),
        (settings.get_testdatadir(), "./testsource"),
        (settings.get_database(), "logs"),
        (settings.get_log_level(), logging.INFO),
    ]
)
def test_settings_funcs(func: object, value: str | bool | int) -> None:
    assert func == value


def test_settings_get_environment_getenv() -> None:
    # Given that the environment var for environment has been set
    os.environ["ENVIRONMENT"] = "prod"

    # When the settings are queried
    settings: Settings = Settings()
    env: str = settings.get_environment()

    # Then it returns the environment var
    assert env == "prod"


def test_settings_get_testing_getenv() -> None:
    # Given that the environment var for testing has been set
    os.environ["TESTING"] = "1"

    # When the settings are queried
    settings: Settings = Settings()
    testing: bool = settings.get_testing()

    # Then it returns the environment var
    assert testing is True
