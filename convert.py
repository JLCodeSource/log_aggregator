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
import csv
import logging
import os
import re
from datetime import datetime

from pydantic import ValidationError

from model import JavaLog

logger = logging.getLogger(__name__)


def line_start_match(match, string):
    # Returns true if the beginning of the string matches match
    matches = bool(re.match(match, string))
    logger.debug(f"Matches: {matches} from {match} with '{string}'")
    return matches


def yield_matches(full_log: list[str]):
    # Yield matches creates a list of logs and yields the list on match
    logs = []
    for line in full_log.split("\n"):
        if line_start_match("INFO|WARN|ERROR", line):  # if line matches start
            if len(logs) > 0:  # if there's already a log
                yield "; ".join(logs)  # yield the log
                logs = []  # and set the log back to nothing
        logs.append(line.strip())  # add current line to log (list)
        logger.debug(f"Appended: {line} to list")


def multi_to_single_line(logfile, target):
    # multiToSingleLine converts multiline to single line logs
    data = open(os.path.join(target, logfile)).read()
    logger.info(f"Opened {logfile} for reading")
    logs = list(yield_matches(data))

    with open(os.path.join(target, logfile), "w") as file:
        for line in logs:
            file.write(f"{line}\n")
            logger.debug(f"Wrote: {line} to {file}")
        logger.info(f"Wrote converted logs to {logfile}")


def convert_log_to_csv(logfile, target):
    # Converts the CSV log file to a dict
    header = ["severity", "jvm", "datetime", "source", "type", "message"]
    with open(os.path.join(target, logfile), "r") as file:
        reader = csv.DictReader(file, delimiter="|", fieldnames=header)
        logger.info(f"Opened {logfile} as csv.dictReader")
        return list(reader)


async def convert(logfile, logs_out, node):
    # Work on log files in logsout
    log_list = []
    for logfile in os.listdir(logs_out):
        multi_to_single_line(logfile, logs_out)
        reader = convert_log_to_csv(logfile, logs_out)

        for dict in reader:
            for k, v in dict.items():
                if dict[k]:
                    try:
                        dict[k] = v.strip()
                    except AttributeError as err:
                        logger.exception(f"AttributeError: {err}")

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
            except ValidationError as err:
                logger.exception(f"ValidationError: {err}")
    return log_list
