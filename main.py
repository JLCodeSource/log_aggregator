"""
Module Name: main.py
Created: 2022-07-24
Creator: JL
Change Log: 2022-07-26 - added environment settings
Summary: Main is an async function that inits the database &
extracts the logs from the source directory
Functions: main, init, extractLog
Variables: sourcedir
"""

from pyparsing import empty
from aggregator.config import get_settings
from aggregator.convert import convert
from aggregator.db import client, init, save_logs
from aggregator.extract import extract_log
from aggregator.logs import configure_logging
import logging

logger = logging.getLogger(__name__)


class Aggregator:
    def __init__(self, func):
        self._func = func

    def __call__(self):
        return self._func()


@Aggregator
async def main():

    try:
        settings = get_settings()
        assert (settings is not empty), "Failed to get settings"
    except AssertionError as err:
        logger.fatal(f"AssertionError: {err}")
        exit()
    logger.info("Loading config settings from the environment...")
    logger.debug(f"Environment: {settings.environment}")
    logger.debug(f"Testing: {settings.testing}")
    logger.debug(f"Connection: {settings.get_connection_log()}")
    logger.debug(f"Sourcedir: {settings.sourcedir}")
    logger.debug(f"Outdir: {settings.outdir}")
    logger.debug(f"Database: {settings.database}")
    logger.info(f"Log Level: {settings.log_level}")
    # Init database
    await init()

    # Extact logs from source directory
    try:
        log_files = await extract_log(settings.sourcedir)
        if log_files is None or log_files is empty:
            raise Exception(
                f"Failed to get log_files from {settings.sourcedir}")
    except Exception as err:
        logger.error(f"{err}")

    for file in log_files:
        log_list = await convert(file)
        await save_logs(log_list)


if __name__ == "__main__":

    configure_logging()

    loop = client.get_io_loop()
    loop.run_until_complete(main())
