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
from aggregator.db import init, insert_logs
from aggregator.extract import extract_log, gen_zip_extract_fn_list
from aggregator.logs import configure_logging
import logging
import asyncio

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
    init_db = asyncio.create_task(init())
    await init_db

    # Create list of configured extraction functions for zip extraction
    log_file_list = gen_zip_extract_fn_list(settings.sourcedir)

    # Extact logs from source directory
    try:
        log_file_list = await extract_log(log_file_list)
        if log_file_list is None or log_file_list is empty:
            raise Exception(
                f"Failed to get log_files from {settings.sourcedir}")
    except Exception as err:
        logger.error(f"{err}")

    convert_fn_list = []
    for log_list in log_file_list:
        for file in log_list:
            convert_fn_list.append(convert(file))

    converted_log_lists = await asyncio.gather(*convert_fn_list)

    insert_log_fn_list = []
    for log_lists in converted_log_lists:
        insert_log_fn_list.append(insert_logs(log_lists))

    ok = await asyncio.gather(*insert_log_fn_list)
    logger.info(f"Output from db insert: {ok}")

if __name__ == "__main__":

    configure_logging()

    asyncio.run(main())
