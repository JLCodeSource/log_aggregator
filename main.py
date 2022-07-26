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

from config import get_settings
from db import client, init
from extract import extract_log
from logs import configure_logging
import logging

logger = logging.getLogger("main")


async def main():

    settings = get_settings()
    logger.info("Loading config settings from the environment...")
    logger.debug(f"Environment: {settings.environment}")
    logger.debug(f"Testing: {settings.testing}")
    logger.debug(f"Connection: {settings.get_connection_log()}")
    logger.debug(f"Sourcedir: {settings.sourcedir}")
    logger.debug(f"Outdir: {settings.outdir}")
    logger.debug(f"Database: {settings.database}")
    # Init database
    await init()

    # Extact logs from source directory
    await extract_log(settings.sourcedir)


if __name__ == "__main__":

    configure_logging()

    loop = client.get_io_loop()
    loop.run_until_complete(main())
