"""
Module Name: convert.py
Created: 2022-07-24
Creator: JL
Change Log: 2022-07-26 - added environment settings
Summary: convert handles conversion of logs into json
for upload to the database.
Functions: lineStartMatch, yield_matches, multiToSingleLine,
convertLogtoCSV, convert
"""
import asyncio
import csv
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Generator

from beanie.exceptions import CollectionWasNotInitialized
from pydantic import ValidationError
from pymongo.errors import ServerSelectionTimeoutError

from aggregator.document import JavaLog
from aggregator.helper import LOG_NODE_PATTERN, get_node

logger: logging.Logger = logging.getLogger(__name__)


def _line_start_match(match: str, string: str) -> bool:
    # Returns true if the beginning of the string matches match
    try:
        matches: bool = bool(re.match(match, string))
        logger.debug(f"Matches: {matches} from {match} with '{string}'")
    except TypeError as err:
        logger.warning(f"TypeError: {err}")
        raise TypeError
    return matches


def _yield_matches(full_log: str) -> Generator:
    # Yield matches creates a list of logs and yields the list on match
    log_tmp: list[str] = []
    for line in full_log.split("\n"):
        line = line.strip()
        if line == "":
            continue
        if _line_start_match("INFO|WARN|ERROR", line):  # if line matches start
            if len(log_tmp) > 0:  # if there's already a log
                log: str = "; ".join(log_tmp)
                yield log  # yield the log
                log_tmp = []  # and set the log back to nothing
        log_tmp.append(line)  # add current line to log (list)
        logger.debug(f"Appended: {line} to list")

    if len(log_tmp) > 0:  # if there's already a log
        log = "; ".join(log_tmp)
    else:
        log = log_tmp[0]
    yield log


def _multi_to_single_line(logfile: Path) -> None:
    # multiToSingleLine converts multiline to single line logs
    data: str = open(logfile).read()
    logger.info(f"Opened {logfile} for reading")
    logs: list[str] = list(_yield_matches(data))

    with open(logfile, "w") as file:
        for line in logs:
            file.write(f"{line}\n")
            logger.debug(f"Wrote: {line} to {file}")
        logger.info(f"Wrote converted logs to {logfile}")


def _strip_whitespace(d: dict) -> dict:
    for k, v in d.items():
        try:
            d[k] = v.strip()
        except AttributeError as err:
            logger.exception(f"AttributeError: {err}")
    return d


def _convert_log_to_csv(logfile: Path) -> list[dict[str | Any, str | Any]]:
    # Converts the CSV log file to a dict
    header: list[str] = ["severity", "jvm", "datetime", "source", "type", "message"]
    with open(logfile, "r") as file:
        reader: csv.DictReader = csv.DictReader(file, delimiter="|", fieldnames=header)
        logger.info(f"Opened {logfile} as csv.dictReader")
        return list(reader)


def _convert_to_datetime(timestamp: str) -> datetime:
    try:
        dt: datetime = datetime.strptime(timestamp, "%Y/%m/%d %H:%M:%S")
    except ValueError as err:
        logger.exception(f"ValueError: {err}")
        raise err
    return dt


async def convert(log_file: Path) -> list[JavaLog]:
    logger.info(f"Starting new convert coroutine for {log_file}")
    # Work on log files in logsout
    log_list: list[JavaLog] = []
    node: str = get_node(log_file, LOG_NODE_PATTERN)

    _multi_to_single_line(log_file)
    reader: list[dict[str | Any, str | Any]] = _convert_log_to_csv(log_file)

    for d in reader:

        d = _strip_whitespace(d)

        d["node"] = node

        if d["message"] is None and d["type"] is None and not d["source"] is None:
            d["message"] = d["source"]
            d["source"] = None

        try:
            timestamp: datetime = _convert_to_datetime(d["datetime"])
            log: JavaLog = JavaLog(
                node=d["node"],
                severity=d["severity"],
                jvm=d["jvm"],
                datetime=timestamp,
                source=d["source"],
                type=d["type"],
                message=d["message"],
            )
            log_list.append(log)
            logger.debug(f"Appended {log} to log_list")
        except (ValueError, ValidationError) as err:
            logger.exception(f"Error {type(err)} {err}")
        except (CollectionWasNotInitialized, ServerSelectionTimeoutError) as err:
            logger.fatal(f"Error: {err=}, {type(err)=}")
            raise err
        except BaseException as err:
            logger.exception(f"Unexpected {err=}, {type(err)=}")
        await asyncio.sleep(0)

    logger.info(f"Ending convert coroutine for {log_file} and {node}")
    return log_list
