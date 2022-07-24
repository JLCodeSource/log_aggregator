"""
Module Name: extract.py 
Created: 2022-07-24
Creator: JL
Change Log: Initial
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

from config import outdir, sourcedir
from convert import convert

logger = logging.getLogger(__name__)


def getNode(file: str) -> str:
    # Extract node name from filename
    node = file.split("_")[1].split(".")[0]
    logger.debug(f"node: {node} from {file}")
    return node


def getLogType(file: str) -> str:
    # Extract logtype from filename
    logtype = file.split("_")[2]
    logger.debug(f"logType: {logtype} from {file}")
    return logtype


def getLogOutputDir(node: str, logtype: str):
    # Return the output dir as a path
    out = os.path.join(outdir, node, logtype)
    logger.debug(f"outdir: {out} from {outdir}, {node}, {logtype}")
    return out


def createLogsOutputDir(target: str):
    # Create logs output directory
    Path(target).mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created {target}")


def extract(file: str, target: str, extension: str):
    # Find zip files and extract (by default) just  files with .log extension
    if file.endswith(".zip"):
        with zipfile.ZipFile(os.path.join(sourcedir, file), "r") as zip_file:
            filesInZip = zip_file.namelist()
            for filename in filesInZip:
                if filename.endswith(extension):
                    zip_file.extract(filename, target)
                    logger.info(
                        f"Extracted {extension} generating {filename} " f"at {target}"
                    )

    # Move log files out of System folder where they are by default
    tmplogsout = os.path.join(target, "System")
    for filename in os.listdir(tmplogsout):
        move(os.path.join(tmplogsout, filename), os.path.join(target, filename))
        logger.debug(f"Moved {filename} from {tmplogsout} to {target}")

    # Remove System folder
    os.rmdir(tmplogsout)
    logger.debug(f"Removed {tmplogsout}")


async def extractLog(dir):
    # Manages the process of extracting the logs
    # Kicks off the conversion process for each in an await

    for file in os.listdir(dir):
        node = getNode(file)
        logtype = getLogType(file)
        logsout = getLogOutputDir(node, logtype)
        extension = "service.log"
        createLogsOutputDir(logsout)

        extract(file, logsout, extension)

        await convert(file, logsout, node)
