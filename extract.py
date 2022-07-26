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

Functions: getNode, getLogType, getLogOutputDir,
createLogsOutputDir, extract, extractLog
"""

import logging
import os
import zipfile
from pathlib import Path
from shutil import move

from config import get_settings
from convert import convert
from db import save_logs

logger = logging.getLogger(__name__)
settings = get_settings()


def get_node(file: str) -> str:
    # Extract node name from filename
    node = file.split("_")[1].split(".")[0]
    logger.debug(f"node: {node} from {file}")
    return node


def get_log_type(file: str) -> str:
    # Extract logtype from filename
    log_type = file.split("_")[2]
    logger.debug(f"log_type: {log_type} from {file}")
    return log_type


def get_log_dir(node: str, log_type: str):
    # Return the output dir as a path
    out = os.path.join(settings.outdir, node, log_type)
    logger.debug(f"outdir: {out} from {settings.outdir}, {node}, {log_type}")
    return out


def create_log_dir(target: str):
    # Create logs output directory
    Path(target).mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created {target}")


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


async def extract(file: str, target: str, extension: str):
    # Find zip files and extract (by default) just  files with .log extension
    with zipfile.ZipFile(os.path.join(
            settings.sourcedir, file), "r") as zip_file:
        filesInZip = zip_file.namelist()
        for filename in filesInZip:
            if filename.endswith(extension):
                zip_file.extract(filename, target)
                logger.info(
                    (f"Extracted *{extension} generating "
                        + f"{filename} at {target}"))

    move_files_to_target(target, "System")

    remove_folder(os.path.join(target, "System"))


async def extract_log(dir):
    # Manages the process of extracting the logs
    # Kicks off the conversion process for each in an await

    for file in os.listdir(dir):
        node = get_node(file)
        log_type = get_log_type(file)
        logs_dir = get_log_dir(node, log_type)
        extension = "service.log"
        create_log_dir(logs_dir)

        if file.endswith(".zip"):
            await extract(file, logs_dir, extension)

        log_list = await convert(file, logs_dir, node)
        await save_logs(log_list)
