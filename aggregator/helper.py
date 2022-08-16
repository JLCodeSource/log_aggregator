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
from aggregator.config import Settings, get_settings

logger: logging.Logger = logging.getLogger(__name__)
settings: Settings = get_settings()


def get_node(file: str) -> str:
    # Extract node name from filename
    if os.path.basename(file).endswith(".zip"):
        node: str = os.path.basename(
            file).split("_")[1].split(".")[0]
    else:
        # Split by directory
        node_split: list[str] = file.split(os.path.sep)
        # node is first directory
        node: str = node_split[-3]
    logger.debug(f"node: {node} from {file}")
    return node


def get_log_type(file: str) -> str:
    # Extract logtype from filename
    if os.path.basename(file).endswith(".zip"):
        log_type: str = os.path.basename(
            file).split("_")[2]
    else:
        # Split by directory
        log_type_split: list[str] = file.split(os.path.sep)
        # log_type is second directory
        log_type: str = log_type_split[-2]
    logger.debug(f"log_type: {log_type} from {file}")
    return log_type


def get_log_dir(node: str, log_type: str) -> str:
    # Return the output dir as a path
    out: str = os.path.join(settings.outdir, node, log_type)
    logger.debug(f"outdir: {out} from {settings.outdir}, {node}, {log_type}")
    return out
