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

from aggregator import config, convert, db, extract, logs, model, view

logger: logging.Logger = logging.getLogger(__name__)


def _get_settings() -> config.Settings:
    try:
        settings: config.Settings | None = config.get_settings()
        assert settings is not None, "Failed to get settings"
    except AssertionError as err:
        logger.fatal(f"AssertionError: {err}")
        raise AssertionError(err)
    logger.info("Loading config settings from the environment...")
    logger.debug(f"Environment: {settings.environment}")
    logger.debug(f"Testing: {settings.testing}")
    logger.debug(f"Connection: {settings.get_connection_log()}")
    logger.debug(f"Sourcedir: {settings.sourcedir}")
    logger.debug(f"Outdir: {settings.outdir}")
    logger.debug(f"Database: {settings.database}")
    logger.info(f"Log Level: {settings.log_level}")

    return settings


async def _init_db() -> AsyncIOMotorClient:
    return await db.init()


async def init_app() -> tuple[AsyncIOMotorClient, config.Settings]:
    # Init settings
    settings: config.Settings = _get_settings()

    # Init database
    client: AsyncIOMotorClient = await _init_db()

    return client, settings


def _get_zip_extract_coro_list(
    settings: config.Settings,
) -> list[Coroutine[Any, Any, list[Path]]]:
    zip_coro_list: list[Coroutine[Any, Any, list[Path]]] = []
    extract.gen_zip_extract_fn_list(settings.sourcedir, zip_coro_list)
    if zip_coro_list is None or zip_coro_list == []:
        err: str = "Zip extract coroutine list is empty"
        logger.error(f"ValueError: {err}")
        raise ValueError(err)
    else:
        coro_list: list[Coroutine[Any, Any, list[Path]]] = cast(
            list[Coroutine[Any, Any, list[Path]]], zip_coro_list
        )
    return coro_list


def _get_convert_coro_list(
    convert_coro_list: list[Coroutine[Any, Any, list[model.JavaLog]]] = [],
    log_file_list: list[str] = [],
) -> list[Coroutine[Any, Any, list[model.JavaLog]]]:
    for log_list in log_file_list:
        for file in log_list:
            convert_coro_list.append(convert.convert(file))
    return convert_coro_list


async def main() -> None:

    client: AsyncIOMotorClient
    settings: config.Settings
    client, settings = await init_app()
    if not isinstance(client, AsyncIOMotorClient):
        exit()

    # Create list of configured extraction functions for zip extraction
    zip_coro_list: list[Coroutine[Any, Any, list[Path]]] = _get_zip_extract_coro_list(
        settings
    )

    # Extact logs from source directory
    try:
        log_file_list: list[str] = await extract.extract_log(zip_coro_list)
        if log_file_list is None or log_file_list is empty:
            raise Exception(f"Failed to get log_files from {settings.sourcedir}")
    except Exception as err:
        logger.error(f"{err}")
        raise Exception(f"Failed to get log_files from {settings.sourcedir}")

    convert_coro_list: list[Coroutine[Any, Any, list[model.JavaLog]]] = []
    convert_coro_list = _get_convert_coro_list(convert_coro_list, log_file_list)

    converted_log_lists: list[list[model.JavaLog]] = await asyncio.gather(
        *convert_coro_list
    )

    insert_log_fn_list: list[Coroutine[Any, Any, InsertManyResult | None]] = []
    for log_lists in converted_log_lists:
        insert_log_fn_list.append(db.insert_logs(log_lists))

    ok: list[InsertManyResult] | None = await asyncio.gather(*insert_log_fn_list)
    logger.info(f"Output from db insert: {ok}")

    out: list[model.JavaLog] = await db.find_logs(query={}, sort="-datetime")
    await view.display_result(out)


if __name__ == "__main__":

    logs.configure_logging()
    # nest_asyncio.apply()
    asyncio.run(main())  # type: ignore #TODO: Update main startup
