"""
Module Name: config.py
Created: 2022-07-24
Creator: JL
Change Log: 2022-07-26 - added environment settings
Summary: config holds the global config settings
Variables: sourcedir, outdir, connection, database
"""

import logging
import os
from functools import lru_cache

from pydantic import BaseSettings  # AnyUrl

logger: logging.Logger = logging.getLogger("__name__")


class Settings(BaseSettings):
    environment: str = os.getenv("ENVIRONMENT", "dev")
    testing: bool = bool(os.getenv("TESTING", 0))
    connection: str = os.getenv(
        "DATABASE_URL",
        "mongodb://root:example@localhost:27017/?authMechanism=DEFAULT")
    sourcedir: str = os.getenv("SOURCE", "./testsource/zips")
    outdir: str = os.getenv("OUT", "./out")
    testdatadir: str = os.getenv("TESTDATA", "./testsource")
    database: str = os.getenv("DATABASE", "logs")
    log_level: int = int(os.getenv("LOG_LEVEL", logging.INFO))

    def get_environment(self) -> str:
        return self.environment

    def get_testing(self) -> bool:
        return self.testing

    def get_connection(self) -> str:
        return self.connection

    def get_connection_log(self) -> str:
        conn_log: str = ""
        conn_log_split: list[str] = self.connection.split("@")
        if len(conn_log_split) > 1:
            url_scheme: str = conn_log_split[0].split("//")[0]
            url_address: str = conn_log_split[1]
            conn_log: str = (f"{url_scheme}//username:password@"
                             f"{url_address}")
        return conn_log

    def get_sourcedir(self) -> str:
        return self.sourcedir

    def get_outdir(self) -> str:
        return self.outdir

    def get_testdatadir(self) -> str:
        return self.testdatadir

    def get_database(self) -> str:
        return self.database

    def get_log_level(self) -> int:
        return self.log_level


@lru_cache()
def get_settings() -> Settings:

    return Settings()
