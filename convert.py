"""convert.py handles conversion of logs into json 
for upload to the database.
"""
import os
import re
import csv
from datetime import datetime

from pydantic import ValidationError
from db import saveLogs
from model import JavaLog


def lineStartMatch(match, string):
    # Returns true if the beginning of the string matches match
    return bool(re.match(match, string))


def yield_matches(full_log: list[str]):
    # Yield matches creates a list of logs and yields the list on match
    log = []
    for line in full_log.split("\n"):
        if lineStartMatch("INFO|WARN|ERROR", line):  # if line matches start
            if len(log) > 0:  # if there's already a log
                yield "; ".join(log)  # yield the log
                log = []  # and set the log back to nothing
        # Handle corrupted lines
        lineLen = len(line.split("|"))
        # if not lineLen == 1 and not lineLen == 6:
        #   print(lineLen)
        #   print(line.split("|"))
        if lineLen > 6:
            line = line.split("|")
            line[5] = line[5] + " ***CORRUPTED_NEXT_LOG_FOLLOWS*** "
            for i in range(6, lineLen):
                line[5] = line[5] + line[i]
                line.pop(i)
            #print("New line: \n {line}".format(line=line))
        tmpLine = ""
        if type(line) == list:
            for s in line:
                tmpLine = tmpLine + "," + s
            line = tmpLine[1:]
            # print(type(line))
            # print(line)
        log.append(line.strip())  # add current line to log (list)


def multiToSingleLine(logfile, target):
    # multiToSingleLine converts multiline to single line logs
    data = open(os.path.join(target, logfile)).read()

    logs = list(yield_matches(data))

    with open(os.path.join(target, logfile), "w") as file:
        for log in logs:
            file.write("{log}\n".format(log=log))


def convertLogtoCSV(logfile, target):
    # Converts the CSV log file to a dict
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
                    try:
                        dict[k] = v.strip()
                    except AttributeError as err:
                        print("Attribute error: {0}".format(err))

            dict["node"] = node

            timestamp = datetime.strptime(
                dict["datetime"].strip(), "%Y/%m/%d %H:%M:%S")
            try:
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
            except ValidationError as err:
                print("Validation error: {err}".format(
                    err=err))

    await saveLogs(LogList)
    LogList = []
    # print(LogList)
