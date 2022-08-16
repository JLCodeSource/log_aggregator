"""
Module Name: extract.py
Created: 2022-07-24
Creator: JL
Change Log: 2022-07-26 - added environment settings
Summary: extract.py handles log file extractions.

It assumes log files have been collected using gbmgm.
Each node has its own log file with the names of the type:
GBLogs_node.domain.tld_servicetype_epochtimestamp.zip

Files are extracted into the "System" directory.
Depending on the type of log, they have different internal name
formats and different log formats.

For example, fanapiservice.zip contains fanapiservice.log and
smb3_1.log and their rolled versions.

Functions: createLogsOutputDir, extract, extractLog
"""

import asyncio
import logging
import os
import zipfile
from pathlib import Path
from shutil import move
from typing import Any, Coroutine, Literal

from aggregator import helper
from aggregator.config import Settings, get_settings

READ: Literal["r"] = "r"
TYPEERROR: str = "Value should not be None"
DEFAULT_LOG_EXTENSION: str = "service.log"

logger: logging.Logger = logging.getLogger(__name__)
settings: Settings = get_settings()


def _create_log_dir(target: str) -> None:
    # Create logs output directory
    try:
        Path(target).mkdir(parents=True, exist_ok=True)
    except (FileNotFoundError, FileExistsError) as err:
        logger.error(f"ErrorType: {type(err)} - Could not create directory")
        raise err
    logger.debug(f"Created {target}")


def _move_files_to_target(target: str, source: str) -> None:
    # Move log files out of System folder where they are by default
    tmp_logs_out: str = os.path.join(target, source)
    for filename in os.listdir(tmp_logs_out):
        move(os.path.join(tmp_logs_out, filename), os.path.join(target, filename))
        logger.debug(f"Moved {filename} from {tmp_logs_out} to {target}")


def _remove_folder(target: str) -> None:
    # Remove System folder
    try:
        os.rmdir(target)
        logger.debug(f"Removed {target}")
    except FileNotFoundError as err:
        logger.error(f"FileNotFoundError: {err}")
        raise err


async def _extract(
    zip_file: str, target_dir: str, extension: str = DEFAULT_LOG_EXTENSION
) -> list[str]:

    logger.info(f"Starting extraction coroutine for {zip_file}")
    log_files: list[str] = []

    if not os.path.exists(zip_file):
        logger.error(f"FileNotFoundError: {zip_file} is not a file")
        raise FileNotFoundError
    elif not zipfile.is_zipfile(zip_file):
        logger.warning(f"BadZipFile: {zip_file} is a BadZipFile")
        raise zipfile.BadZipFile

    # Find zip files and extract (by default) just  files with .log extension
    with zipfile.ZipFile(zip_file, READ) as zf:

        filesInZip: list[str] = zf.namelist()
        for filename in filesInZip:
            if filename.endswith(extension):
                await asyncio.sleep(0)
                zf.extract(filename, target_dir)
                logger.info(
                    f"Extracted *{extension} generating {filename} at {target_dir}"
                )

        # TODO: Extract move_files_to_target & remove_folder
        _move_files_to_target(target_dir, "System")

        _remove_folder(os.path.join(target_dir, "System"))

        for filename in os.listdir(target_dir):
            filename = os.path.join(target_dir, filename)
            log_files.append(filename)

    logger.info(f"Ending extraction coroutine for {zip_file}")
    return log_files


def gen_zip_extract_fn_list(
    src_dir: str,
    zip_files_extract_fn_list: list[Coroutine[Any, Any, list[str]]] | None = [],
) -> list[Coroutine[Any, Any, list[str]]] | None:
    # Manages the process of extracting the logs
    # Kicks off the conversion process for each in an await
    # Added options to pass in list values for testing purposes

    for zip_file in os.listdir(src_dir):
        try:
            node: str = helper.get_node(zip_file)
            log_type: str = helper.get_log_type(zip_file)
            logs_dir: str = helper.get_log_dir(node, log_type)
            if node is None or log_type is None or logs_dir is None:
                raise TypeError(TYPEERROR)
        except TypeError as err:
            logger.error(f"TypeError: {err}")
            raise err

        _create_log_dir(logs_dir)
        zip_file = os.path.join(src_dir, zip_file)

        try:
            zip_files_extract_fn_list.append(  # type: ignore
                _extract(zip_file, logs_dir)
            )
        except AttributeError as err:
            logger.error(f"Attribute Error: {err}")
            raise err

    return zip_files_extract_fn_list


async def extract_log(
    extract_fn_list: list[Coroutine[Any, Any, list[str]]],
    log_files: list[str] = [],
) -> list[str]:

    try:
        new_log_files: list = await asyncio.gather(*extract_fn_list)
        log_files.extend(list(new_log_files))
    except (FileNotFoundError, TypeError) as err:
        logger.error(f"ErrorType: {type(err)} - asyncio gather failed")
        raise err

    return log_files
