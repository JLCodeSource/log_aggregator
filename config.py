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

from pydantic import AnyUrl, BaseSettings

logger = logging.getLogger("__name__")


class Settings(BaseSettings):
    environment: str = os.getenv("ENVIRONMENT", "dev")
    testing: bool = os.getenv("TESTING", 0)
    connection: AnyUrl = os.getenv(
        "DATABASE_URL",
        "mongodb://root:example@localhost:27017/?authMechanism=DEFAULT"
    )
    sourcedir: str = os.getenv("SOURCE", "./source")
    outdir: str = os.getenv("OUT", "./out")
    database: str = os.getenv("DATABASE", "logs")
    log_level: int = os.getenv("LOG_LEVEL", logging.DEBUG)

    def get_environment(self):
        return self.environment

    def get_testing(self):
        return self.testing

    def get_connection(self):
        return self.connection

    def get_connection_log(self):
        conn_log = self.connection.split("@")
        if len(conn_log) > 1:
            conn_log[0] = conn_log[0].split("//")
            conn_log = str(conn_log[0][0]) + "//username:password" \
                + str(conn_log[1:])
        return conn_log

    def get_sourcedir(self):
        return self.sourcedir

    def get_outdir(self):
        return self.outdir

    def get_database(self):
        return self.database

    def get_loglevel(self):
        return self.log_level


@lru_cache()
def get_settings() -> BaseSettings:

    return Settings()
