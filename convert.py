import os
import re
import csv
from datetime import datetime
from db import saveLogs
from model import JavaLog


def lineStartMatch(match, string):
    """Returns true if the beginning of the string matches match"""
    return bool(re.match(match, string))


def yield_matches(full_log: list[str]):
    log = []
    for line in full_log.split("\n"):
        if lineStartMatch("INFO|WARN|ERROR", line):  # if line matches start
            if len(log) > 0:  # if there's already a log
                yield "; ".join(log)  # yield the log
                log = []  # and set the log back to nothing
        if not len(line.split("|")) == 6:
            print(len(line.split("|")))
        log.append(line.strip())  # add current line to log (list)


def multiToSingleLine(logfile, target):
    data = open(os.path.join(target, logfile)).read()

    logs = list(yield_matches(data))

    with open(os.path.join(target, logfile), "w") as file:
        for log in logs:
            file.write("{log}\n".format(log=log))


def convertLogtoCSV(logfile, target):
    header = ["severity", "jvm",
              "datetime", "source", "type", "message"]
    with open(os.path.join(target, logfile), "r") as file:
        reader = csv.DictReader(file, delimiter="|", fieldnames=header)
        return list(reader)


async def convert(logfile, logsout, node):
    # Work on log files in logsout

    LogList = []
    for logfile in os.listdir(logsout):
        multiToSingleLine(logfile, logsout)
        reader = convertLogtoCSV(logfile, logsout)

        for dict in reader:
            for k, v in dict.items():
                if dict[k]:
                    dict[k] = v.strip()

            dict["node"] = node

            timestamp = datetime.strptime(
                dict["datetime"].strip(), "%Y/%m/%d %H:%M:%S")
            log = JavaLog(
                node=dict["node"],
                severity=dict["severity"],
                jvm=dict["jvm"],
                datetime=timestamp,
                source=dict["source"],
                type=dict["type"],
                message=dict["message"]
            )
            LogList.append(log)

    await saveLogs(LogList)
    LogList = []
    # print(LogList)
