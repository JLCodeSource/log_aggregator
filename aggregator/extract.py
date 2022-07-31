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


logger = logging.getLogger(__name__)
settings = get_settings()


def create_log_dir(target: str):
    # Create logs output directory
    try:
        Path(target).mkdir(parents=True, exist_ok=True)
        logger.debug(f"Created {target}")
    except FileNotFoundError as err:
        logger.error(f"Could not create directory: {err}")
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


async def extract(file: str, target: os.path, extension: str) -> list:

    logger.info(f"Starting extraction coroutine for {file}")
    log_files = []
    # Find zip files and extract (by default) just  files with .log extension
    with zipfile.ZipFile(os.path.join(
            settings.sourcedir, file), "r") as zip_file:
        filesInZip = zip_file.namelist()
        for filename in filesInZip:
            if filename.endswith(extension):
                await asyncio.sleep(0)
                zip_file.extract(filename, target)
                logger.info(
                    (f"Extracted *{extension} generating "
                        + f"{filename} at {target}"))

    move_files_to_target(target, "System")

    remove_folder(os.path.join(target, "System"))

    for filename in os.listdir(target):
        log_files.append(os.path.join(target, filename))

    logger.info(f"Ending extraction coroutine for {file}")
    return log_files


def gen_zip_extract_fn_list(
        dir: os.path,
        zip_files_extract_fn_list: list | None = []) -> list:
    # Manages the process of extracting the logs
    # Kicks off the conversion process for each in an await
    # Added options to pass in list values for testing purposes

    for file in os.listdir(dir):
        try:
            node = helper.get_node(file)
            if node is None:
                raise Exception("TypeError", "node should not be 'NoneType'")
        except Exception as err:
            logger.error(f"TypeError: {err}")
            continue
        try:
            log_type = helper.get_log_type(file)
            if log_type is None:
                raise Exception("NoneException", "log_type should not be None")
        except Exception as err:
            logger.error(f"NoneException: {err}")
            exit()
        try:
            logs_dir = helper.get_log_dir(node, log_type)
            if logs_dir is None:
                raise Exception("NoneException", "logs_dir should not be None")
        except Exception as err:
            logger.error(f"NoneException: {err}")
            exit()

        extension = "service.log"
        create_log_dir(logs_dir)

        if file.endswith(".zip"):
            try:
                zip_files_extract_fn_list.append(
                    extract(file, logs_dir, extension))
            except AttributeError as err:
                logger.error(f"Attribute Error: {err}")
                raise err
        else:
            continue
    return zip_files_extract_fn_list


async def extract_log(
        extract_fn_list: list = [], log_files: list = []) -> list:

    try:
        new_log_files = await asyncio.gather(*extract_fn_list)
    except FileNotFoundError as err:
        logger.error(f"FileNotFound Error: {err}")
        raise err
    log_files.extend(list(new_log_files))

    return log_files
