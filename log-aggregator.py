import os
import zipfile
import fileinput
import sys
#import csv
from pathlib import Path
from shutil import move

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


def enrichLog(logfile, target, node):
    prepend = node + '\t| '

    for line in fileinput.input([os.path.join(target, logfile)],
                                inplace=True):
        sys.stdout.write("{prepend}{line}".format(
            prepend=prepend, line=line))


def main():
    # Main app
    for file in os.listdir(sourcedir):
        node = getNode(file)
        logtype = getLogType(file)
        logsout = getLogOutputDir(node, logtype)
        extension = ".log"
        createLogsOutputDir(logsout)

        extractLog(file, logsout, extension)

    # Work on log files in logsout
    for logfile in os.listdir(logsout):
        enrichLog(logfile, logsout, node)


main()
