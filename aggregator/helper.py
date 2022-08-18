"""
Module Name: helper.py
Created: 2022-07-27
Creator: JL
Change Log: Initial
Summary: helper.py provides helper functions.

It assumes archive logs have been collected using gbmgm.
Each node has its own log file with the names of the type:
GBLogs_node.domain.tld_servicetype_epochtimestamp.zip

Files are extracted into the "System" directory.
Depending on the type of log, they have different internal name
formats and different log formats.

For example, fanapiservice.zip contains fanapiservice.log and
smb3_1.log and their rolled versions.

The structure of the .log files is outdir/node/service/log.*

Functions: getNode, getLogType, getLogOutputDir,
"""

import logging
import os
import re
from pathlib import Path
from typing import Pattern

from aggregator.config import Settings, get_settings

ZIP_NODE_PATTERN: Pattern = re.compile(
    r"^.+\/.+[L][o][g][s]_(.+?)([.].+|)_.+_\d{13}[.][z][i][p]$"
)
LOG_NODE_PATTERN: Pattern = re.compile(
    r"^.+\/([^\/].+?)([.].+|)\/.+\/.+[.][l][o][g](\d|)$"
)
ZIP_LOGTYPE_PATTERN: Pattern = re.compile(
    r"^.+\/.+[L][o][g][s]_.+_(.+?)_\d{13}[.][z][i][p]$"
)
LOG_LOGTYPE_PATTERN: Pattern = re.compile(r"^.+\/.+\/([^\/].+)\/.+[.][l][o][g](\d|)$")


logger: logging.Logger = logging.getLogger(__name__)
settings: Settings = get_settings()


def get_node(file: Path, pattern: Pattern) -> str:
    # Extract node name from filename
    match: re.Match[str] | None = re.match(pattern, str(file))
    if match is None:
        logger.warning(
            f"Wrong filename structure when getting node from {file} with {pattern}"
        )
        return ""
    else:
        node: str = match[1]
        logger.debug(f"node: {node} from {file}")
    return node


def get_log_type(file: Path, pattern: Pattern) -> str:
    # Extract logtype from filename
    match: re.Match[str] | None = re.match(pattern, str(file))
    if match is None:
        logger.warning(
            f"Wrong filename structure when getting logtype from {file} with {pattern}"
        )
        return ""
    else:
        log_type: str = match[1]
    logger.debug(f"log_type: {log_type} from {file}")
    return log_type


def get_log_dir(node: str, log_type: str) -> Path:
    # Return the output dir as a path
    out: Path = Path(os.path.join(settings.outdir, node, log_type))
    logger.debug(f"outdir: {out} from {settings.outdir}, {node}, {log_type}")
    return out
