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

import asyncio
import logging
from pathlib import Path
from typing import Any, Coroutine, cast

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.results import InsertManyResult
from pyparsing import empty

from aggregator.config import Settings, get_settings
from aggregator.convert import convert
from aggregator.db import find_logs, init, insert_logs
from aggregator.extract import extract_log, gen_zip_extract_fn_list
from aggregator.logs import configure_logging
from aggregator.model import JavaLog
from aggregator.view import display_result

logger: logging.Logger = logging.getLogger(__name__)


class Aggregator:
    def __init__(self, func) -> None:
        self._func = func

    def __call__(self) -> object:
        return self._func()


async def init_app() -> tuple[AsyncIOMotorClient, Settings]:
    try:
        settings: Settings = get_settings()
        assert settings is not empty, "Failed to get settings"
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
    init_db: asyncio.Task[AsyncIOMotorClient] = asyncio.create_task(init())
    result: AsyncIOMotorClient = await init_db
    return result, settings


def _get_zip_extract_coro_list(
    settings: Settings,
) -> list[Coroutine[Any, Any, list[Path]]]:
    zip_coro_list: list[Coroutine[Any, Any, list[Path]]] = []
    gen_zip_extract_fn_list(settings.sourcedir, zip_coro_list)
    if zip_coro_list is None:
        raise ValueError
    else:
        coro_list: list[Coroutine[Any, Any, list[Path]]] = cast(
            list[Coroutine[Any, Any, list[Path]]], zip_coro_list
        )
    return coro_list


def _get_convert_coro_list(
    convert_coro_list: list[Coroutine[Any, Any, list[JavaLog]]] = [],
    log_file_list: list[str] = [],
) -> list[Coroutine[Any, Any, list[JavaLog]]]:
    for log_list in log_file_list:  # type: ignore
        for file in log_list:
            convert_coro_list.append(convert(Path(file)))
    return convert_coro_list


@Aggregator
async def main() -> None:

    client: AsyncIOMotorClient
    settings: Settings
    client, settings = await init_app()
    if not isinstance(client, AsyncIOMotorClient):
        exit()

    # Create list of configured extraction functions for zip extraction
    zip_coro_list: list[Coroutine[Any, Any, list[Path]]] = _get_zip_extract_coro_list(
        settings
    )

    # Extact logs from source directory
    try:
        log_file_list: list[Path] = await extract_log(zip_coro_list)
        if log_file_list is None or log_file_list is empty:
            raise Exception(f"Failed to get log_files from {settings.sourcedir}")
    except Exception as err:
        logger.error(f"{err}")

    convert_coro_list: list[Coroutine[Any, Any, list[JavaLog]]] = []
    convert_coro_list = _get_convert_coro_list(
        convert_coro_list, log_file_list  # type: ignore
    )

    converted_log_lists: list[list[JavaLog]] = await asyncio.gather(*convert_coro_list)

    insert_log_fn_list: list[Coroutine[Any, Any, InsertManyResult | None]] = []
    for log_lists in converted_log_lists:
        insert_log_fn_list.append(insert_logs(log_lists))

    ok: list[InsertManyResult] | None = await asyncio.gather(*insert_log_fn_list)
    logger.info(f"Output from db insert: {ok}")

    out: list[JavaLog] = await find_logs(query={}, sort="-datetime")
    await display_result(out)


if __name__ == "__main__":

    configure_logging()

    asyncio.run(main())  # type: ignore #TODO: Update main startup
