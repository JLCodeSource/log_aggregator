
import os
import zipfile
import csv
from pathlib import Path
from shutil import move
from datetime import datetime
import re
from model import JavaLog
from db import init, client

# Vars
sourcedir = "./source"
outdir = "./out"


def getNode(file: str) -> str:
    # Extract node name from example:
    #GBLogs_node.domain.tld_fanapiservice_1657563223771.zip
    return file.split("_")[1].split(".")[0]


def getLogType(file: str) -> str:
    # Extract logtype
    return file.split("_")[2]


def getLogOutputDir(node: str, logtype: str):
    return os.path.join(outdir, node, logtype)


def createLogsOutputDir(target: str):
    # Create logs output directory
    Path(target).mkdir(parents=True, exist_ok=True)


def extractLog(file: str, target: str, extension: str):
    # Find zip files and extract (by default) just  files with .log extension
    if file.endswith(".zip"):
        with zipfile.ZipFile(os.path.join(sourcedir, file), 'r') as zip_file:
            filesInZip = zip_file.namelist()
            for filename in filesInZip:
                if filename.endswith(extension):
                    zip_file.extract(filename, target)

    # Move log files out of System folder where they are by default
    tmplogsout = os.path.join(target, "System")
    for filename in os.listdir(tmplogsout):
        move(os.path.join(tmplogsout, filename),
             os.path.join(target, filename))

    # Remove System folder
    os.rmdir(tmplogsout)


def lineStartMatch(match, string):
    """Returns true if the beginning of the string matches match"""
    return bool(re.match(match, string))


def yield_matches(full_log):
    log = []
    for line in full_log.split("\n"):
        if lineStartMatch("INFO|WARN|ERROR", line):  # if line matches start
            if len(log) > 0:  # if there's already a log
                yield "; ".join(log)  # yield the log
                log = []  # and set the log back to nothing
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


async def saveLogs(logs):
    await JavaLog.insert_many(logs)


async def main():
    # Main app

    await init()

    LogList = []
    for file in os.listdir(sourcedir):
        node = getNode(file)
        logtype = getLogType(file)
        logsout = getLogOutputDir(node, logtype)
        extension = "service.log"
        createLogsOutputDir(logsout)

        extractLog(file, logsout, extension)

        # Work on log files in logsout
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

                # print(LogList)

            await saveLogs(LogList)


main()


if __name__ == "__main__":
    loop = client.get_io_loop()
    loop.run_until_complete(main())
