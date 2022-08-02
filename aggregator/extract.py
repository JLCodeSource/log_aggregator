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

from aggregator import helper
from aggregator.config import get_settings


READ = "r"
TYPEERROR = "Value should not be None"
DEFAULT_LOG_EXTENSION = "service.log"

logger = logging.getLogger(__name__)
settings = get_settings()


def create_log_dir(target: str):
    # Create logs output directory
    try:
        Path(target).mkdir(parents=True, exist_ok=True)
        logger.debug(f"Created {target}")
    except (FileNotFoundError, FileExistsError) as err:
        logger.error(f"ErrorType: {type(err)} - Could not create directory")
        raise err


def move_files_to_target(target: str, source: str):
    # Move log files out of System folder where they are by default
    tmp_logs_out = os.path.join(target, source)
    for filename in os.listdir(tmp_logs_out):
        move(os.path.join(tmp_logs_out, filename),
             os.path.join(target, filename))
        logger.debug(f"Moved {filename} from {tmp_logs_out} to {target}")


def remove_folder(target):
    # Remove System folder
    os.rmdir(target)
    logger.debug(f"Removed {target}")


async def extract(
        file: str, target: os.path,
        extension: str = DEFAULT_LOG_EXTENSION) -> list:

    logger.info(f"Starting extraction coroutine for {file}")
    log_files = []
    # Find zip files and extract (by default) just  files with .log extension
    with zipfile.ZipFile(os.path.join(
            settings.sourcedir, file), READ) as zip_file:
        filesInZip = zip_file.namelist()
        for filename in filesInZip:
            if filename.endswith(extension):
                await asyncio.sleep(0)
                zip_file.extract(filename, target)
                logger.info(
                    f"Extracted *{extension} generating {filename} at {target}"
                )
    # TODO: Extract move_files_to_target & remove_folder
    move_files_to_target(target, "System")

    remove_folder(os.path.join(target, "System"))

    for filename in os.listdir(target):
        log_files.append(os.path.join(target, filename))

    logger.info(f"Ending extraction coroutine for {file}")
    return log_files


def gen_zip_extract_fn_list(
        dir: os.path,
        zip_files_extract_fn_list: list | None = []) -> list | Exception:
    # Manages the process of extracting the logs
    # Kicks off the conversion process for each in an await
    # Added options to pass in list values for testing purposes

    for file in os.listdir(dir):
        try:
            node = helper.get_node(file)
            log_type = helper.get_log_type(file)
            logs_dir = helper.get_log_dir(node, log_type)
            if node is None or \
                    log_type is None or \
                    logs_dir is None:
                raise TypeError(TYPEERROR)
        except TypeError as err:
            logger.error(f"TypeError: {err}")
            return err

        create_log_dir(logs_dir)

        try:
            zip_files_extract_fn_list.append(
                extract(file, logs_dir))
        except AttributeError as err:
            logger.error(f"Attribute Error: {err}")
            raise err

    return zip_files_extract_fn_list


async def extract_log(
    extract_fn_list: list = [],
    log_files: list = []
) -> list:

    try:
        new_log_files = await asyncio.gather(*extract_fn_list)
        log_files.extend(list(new_log_files))
    except (FileNotFoundError, TypeError) as err:
        logger.error(f"ErrorType: {type(err)} - asyncio gather failed")
        raise err

    return log_files
