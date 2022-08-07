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
import os
import re
from datetime import datetime

from pydantic import ValidationError
from beanie.exceptions import CollectionWasNotInitialized
from aggregator.helper import get_node

from aggregator.model import JavaLog

logger = logging.getLogger(__name__)


def line_start_match(match: str, string: str) -> bool:
    # Returns true if the beginning of the string matches match
    try:
        matches = bool(re.match(match, string))
        logger.debug(f"Matches: {matches} from {match} with '{string}'")
    except TypeError as err:
        logger.warning(f"TypeError: {err}")
        raise TypeError
    return matches


def yield_matches(full_log: list[str]):
    # Yield matches creates a list of logs and yields the list on match
    logs = []
    for line in full_log.split("\n"):
        if line_start_match("INFO|WARN|ERROR", line):  # if line matches start
            if len(logs) > 0:  # if there's already a log
                tmp_line = "; ".join(logs)
                yield tmp_line  # yield the log
                logs = []  # and set the log back to nothing
        logs.append(line.strip())  # add current line to log (list)
        logger.debug(f"Appended: {line} to list")

    yield line


def multi_to_single_line(logfile: os.path) -> None:
    # multiToSingleLine converts multiline to single line logs
    data = open(logfile).read()
    logger.info(f"Opened {logfile} for reading")
    logs = list(yield_matches(data))

    with open(os.path.join(logfile), "w") as file:
        for line in logs:
            file.write(f"{line}\n")
            logger.debug(f"Wrote: {line} to {file}")
        logger.info(f"Wrote converted logs to {logfile}")


def convert_log_to_csv(logfile) -> list[dict]:
    # Converts the CSV log file to a dict
    header = ["severity", "jvm", "datetime", "source", "type", "message"]
    with open(os.path.join(logfile), "r") as file:
        reader = csv.DictReader(file, delimiter="|", fieldnames=header)
        logger.info(f"Opened {logfile} as csv.dictReader")
        return list(reader)


def strip_whitespace(d: dict) -> dict:
    for k, v in d.items():
        try:
            d[k] = v.strip()
        except AttributeError as err:
            logger.exception(f"AttributeError: {err}")
    return d


async def convert(log_file: os.path) -> list[JavaLog]:
    logger.info(f"Starting new convert coroutine for {log_file}")
    # Work on log files in logsout
    log_list = []
    node = get_node(log_file)

    multi_to_single_line(log_file)
    reader = convert_log_to_csv(log_file)

    for dict in reader:

        dict = strip_whitespace(dict)

        dict["node"] = node

        timestamp = datetime.strptime(dict["datetime"].strip(),
                                      "%Y/%m/%d %H:%M:%S")

        if (
            dict["message"] is None
            and dict["type"] is None
            and not dict["source"] is None
        ):
            dict["message"] = dict["source"]
            dict["source"] = None

        try:
            log = JavaLog(
                node=dict["node"],
                severity=dict["severity"],
                jvm=dict["jvm"],
                datetime=timestamp,
                source=dict["source"],
                type=dict["type"],
                message=dict["message"],
            )
            log_list.append(log)
            logger.debug(f"Appended {log} to log_list")
        except ValidationError as err:
            logger.exception(f"ValidationError: {err}")
        except CollectionWasNotInitialized as err:
            logger.fatal(f"CollectionWasNotInitializedError: {err}")
            exit()
        except BaseException as err:
            logger.exception(f"Unexpected {err=}, {type(err)=}")
        await asyncio.sleep(0)

    logger.info(f"Ending convert coroutine for {log_file} and {node}")
    return log_list
